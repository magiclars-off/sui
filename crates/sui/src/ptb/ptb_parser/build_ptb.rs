// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// TODO:
// * [x] Handle more pure arguments (especially addresses)
// * [ ] Handle passing non-direct arrays of values to commands.
// * [ ] Integrate keytool for resolution of addresses

use std::collections::BTreeMap;

use anyhow::Result;
use async_recursion::async_recursion;
use async_trait::async_trait;
use move_binary_format::{
    access::ModuleAccess, binary_views::BinaryIndexedView, file_format::SignatureToken,
    file_format_common::VERSION_MAX,
};
use move_core_types::ident_str;
use move_package::BuildConfig;
use serde::Serialize;
use sui_json::is_receiving_argument;
use sui_json_rpc_types::{SuiObjectData, SuiObjectDataOptions, SuiRawData};
use sui_protocol_config::ProtocolConfig;
use sui_sdk::apis::ReadApi;
use sui_types::{
    base_types::ObjectID,
    move_package::MovePackage,
    object::Owner,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    resolve_address,
    transaction::{self as Tx, ObjectArg},
    Identifier, TypeTag, SUI_FRAMEWORK_PACKAGE_ID,
};

use crate::{
    client_commands::{compile_package, upgrade_package},
    err, error,
    ptb::ptb_parser::{
        argument::Argument as PTBArg,
        command_token::CommandToken,
        context::{FileScope, PTBContext},
        errors::PTBResult,
        parser::ParsedPTBCommand,
    },
};

pub struct GasBudget {
    pub gas_budgets: Vec<u64>,
    pub picker: Option<GasPicker>,
}

pub enum GasPicker {
    Max,
    Min,
    Sum,
}

#[async_trait]
trait Resolver<'a>: Send {
    async fn pure<T: Serialize + Send>(
        &mut self,
        builder: &mut PTBBuilder<'a>,
        x: T,
    ) -> PTBResult<Tx::Argument>;
    async fn resolve_object_id(
        &mut self,
        builder: &mut PTBBuilder<'a>,
        x: ObjectID,
    ) -> PTBResult<Tx::Argument>;
}

struct ToObject {
    is_receiving: bool,
    is_mut: bool,
}

impl Default for ToObject {
    fn default() -> Self {
        Self {
            is_receiving: false,
            is_mut: true,
        }
    }
}

impl ToObject {
    fn new(is_receiving: bool) -> Self {
        Self {
            is_receiving,
            // TODO: Make mutability decision be passed in from calling context.
            // For now we assume all uses of shared objects are mutable.
            is_mut: true,
        }
    }
}

#[async_trait]
impl<'a> Resolver<'a> for ToObject {
    async fn pure<T: Serialize + Send>(
        &mut self,
        builder: &mut PTBBuilder<'a>,
        x: T,
    ) -> PTBResult<Tx::Argument> {
        builder.ptb.pure(x).map_err(|e| err!(builder, "{e}"))
    }
    async fn resolve_object_id(
        &mut self,
        builder: &mut PTBBuilder<'a>,
        obj_id: ObjectID,
    ) -> PTBResult<Tx::Argument> {
        let obj = builder.get_object(obj_id).await?;
        let owner = obj
            .owner
            .ok_or_else(|| err!(builder, "Unable to get owner info for object {obj_id}"))?;
        let object_ref = obj.object_ref();
        let obj_arg = match owner {
            Owner::AddressOwner(_) if self.is_receiving => ObjectArg::Receiving(object_ref),
            Owner::Immutable | Owner::AddressOwner(_) => ObjectArg::ImmOrOwnedObject(object_ref),
            Owner::Shared {
                initial_shared_version,
            } => ObjectArg::SharedObject {
                id: object_ref.0,
                initial_shared_version,
                mutable: self.is_mut,
            },
            Owner::ObjectOwner(_) => error!(
                builder,
                "Tried to use an object-owned object as an argument",
            ),
        };
        builder.ptb.obj(obj_arg).map_err(|e| err!(builder, "{e}"))
    }
}

struct ToPure;

#[async_trait]
impl<'a> Resolver<'a> for ToPure {
    async fn pure<T: Serialize + Send>(
        &mut self,
        builder: &mut PTBBuilder<'a>,
        x: T,
    ) -> PTBResult<Tx::Argument> {
        builder.ptb.pure(x).map_err(|e| err!(builder, "{e}"))
    }
    async fn resolve_object_id(
        &mut self,
        builder: &mut PTBBuilder<'a>,
        x: ObjectID,
    ) -> PTBResult<Tx::Argument> {
        builder.ptb.pure(x).map_err(|e| err!(builder, "{e}"))
    }
}

struct NoResolution;

#[async_trait]
impl<'a> Resolver<'a> for NoResolution {
    async fn pure<T: Serialize + Send>(
        &mut self,
        builder: &mut PTBBuilder<'a>,
        x: T,
    ) -> PTBResult<Tx::Argument> {
        builder.ptb.pure(x).map_err(|e| err!(builder, "{e}"))
    }
    async fn resolve_object_id(
        &mut self,
        builder: &mut PTBBuilder<'a>,
        _x: ObjectID,
    ) -> PTBResult<Tx::Argument> {
        error!(builder, "Don't resolve arguments and that's fine");
    }
}

pub struct PTBBuilder<'a> {
    pub identifiers: BTreeMap<Identifier, Vec<FileScope>>,
    pub arguments_to_resolve: BTreeMap<Identifier, PTBArg>,
    pub resolved_arguments: BTreeMap<Identifier, Tx::Argument>,
    pub ptb: ProgrammableTransactionBuilder,
    pub reader: &'a ReadApi,
    pub last_command: Option<Tx::Argument>,
    pub context: PTBContext,
    pub gas_budget: GasBudget,
    pub preview_set: bool,
    pub warn_on_shadowing: bool,
}

pub enum ResolutionResult {
    Resolved(Tx::Argument),
    Raw(PTBArg),
    BoundRaw(Identifier, PTBArg),
}

impl GasBudget {
    pub fn new() -> Self {
        Self {
            gas_budgets: vec![],
            picker: None,
        }
    }

    pub fn finalize(&self) -> anyhow::Result<u64> {
        if self.gas_budgets.is_empty() {
            anyhow::bail!("No gas budget set");
        }

        let budget = if self.gas_budgets.len() == 1 {
            self.gas_budgets[0]
        } else {
            match self.picker {
                Some(GasPicker::Max) => self.gas_budgets.iter().max().unwrap().clone(),
                Some(GasPicker::Min) => self.gas_budgets.iter().min().unwrap().clone(),
                Some(GasPicker::Sum) => self.gas_budgets.iter().sum(),
                None => anyhow::bail!("No gas picker set"),
            }
        };

        if budget == 0 {
            anyhow::bail!("Invalid gas budget");
        }
        Ok(budget)
    }

    pub fn add_gas_budget(&mut self, budget: u64) {
        self.gas_budgets.push(budget);
    }

    pub fn set_gas_picker(&mut self, picker: GasPicker) -> Result<()> {
        if self.picker.is_some() {
            anyhow::bail!("Gas picker already set");
        }
        self.picker = Some(picker);
        Ok(())
    }
}

// Look at `is_primitive` in sui_types and update here
fn is_pure(t: &TypeTag) -> anyhow::Result<bool> {
    Ok(match t {
        TypeTag::Bool
        | TypeTag::U8
        | TypeTag::U64
        | TypeTag::U128
        | TypeTag::Address
        | TypeTag::Vector(_)
        | TypeTag::U16
        | TypeTag::U32
        | TypeTag::U256 => true,
        TypeTag::Struct(_) => false,
        TypeTag::Signer => anyhow::bail!("Signer is not a valid type"),
    })
}

impl<'a> PTBBuilder<'a> {
    pub fn new(reader: &'a ReadApi) -> Self {
        Self {
            identifiers: BTreeMap::new(),
            arguments_to_resolve: BTreeMap::new(),
            resolved_arguments: BTreeMap::new(),
            ptb: ProgrammableTransactionBuilder::new(),
            reader,
            last_command: None,
            context: PTBContext::new(),
            gas_budget: GasBudget::new(),
            preview_set: false,
            warn_on_shadowing: false,
        }
    }

    pub fn declare_identifier(&mut self, ident: Identifier) {
        let current_context = self.context.current_file_scope().clone();
        let e = self.identifiers.entry(ident).or_default();
        e.push(current_context);
    }

    pub fn finish(self) -> PTBResult<(Tx::ProgrammableTransaction, u64, bool)> {
        let budget = self.gas_budget.finalize().map_err(|e| err!(self, "{e}"))?;
        if self.warn_on_shadowing {
            for (ident, commands) in self.identifiers.iter() {
                if commands.len() > 1 {
                    eprintln!(
                        "WARNING: identifier {:?} is shadowed in commands {:?}",
                        ident, commands
                    );
                }
            }
        }
        let ptb = self.ptb.finish();
        Ok((ptb, budget, self.preview_set))
    }

    async fn resolve_to_package(&mut self, package_id: ObjectID) -> PTBResult<MovePackage> {
        let object = self
            .reader
            .get_object_with_options(package_id, SuiObjectDataOptions::bcs_lossless())
            .await
            .map_err(|e| err!(self, "{e}"))?
            .into_object()
            .map_err(|e| err!(self, "{e}"))?;
        let Some(SuiRawData::Package(package)) = object.bcs else {
            error!(
                self,
                "Bcs field in object [{}] is missing or not a package.", package_id
            );
        };
        let package: MovePackage = MovePackage::new(
            package.id,
            object.version,
            package.module_map,
            ProtocolConfig::get_for_min_version().max_move_package_size(),
            package.type_origin_table,
            package.linkage_table,
        )
        .map_err(|e| err!(self, "{e}"))?;
        Ok(package)
    }

    async fn resolve_move_call_arg(
        &mut self,
        view: &BinaryIndexedView<'_>,
        ty_args: &[TypeTag],
        arg: PTBArg,
        param: &SignatureToken,
    ) -> PTBResult<Tx::Argument> {
        // See if we've already resolved this argument or if it's an unambiguously pure value
        if let Ok(res) = self.resolve(arg.clone(), NoResolution).await {
            return Ok(res);
        }

        // Otherwise it'a ambiguous what the value should be, and we need to turn to the signature
        // to determine it.
        let mut is_object_arg = false;
        let mut is_receiving = false;

        for tok in param.preorder_traversal() {
            match tok {
                SignatureToken::Struct(..) | SignatureToken::StructInstantiation(..) => {
                    is_object_arg = true;
                    is_receiving |= is_receiving_argument(view, tok);
                    break;
                }
                SignatureToken::TypeParameter(idx) => {
                    let Some(tag) = ty_args.get(*idx as usize) else {
                        error!(self, "Not enough type parameters supplied for Move call",);
                    };
                    if !is_pure(tag).map_err(|e| err!(self, "{e}"))? {
                        is_object_arg = true;
                        break;
                    }
                }
                SignatureToken::Bool
                | SignatureToken::U8
                | SignatureToken::U64
                | SignatureToken::U128
                | SignatureToken::Address
                | SignatureToken::Signer
                | SignatureToken::Vector(_)
                | SignatureToken::U16
                | SignatureToken::U32
                | SignatureToken::U256
                | SignatureToken::Reference(_)
                | SignatureToken::MutableReference(_) => {}
            }
        }

        if is_object_arg {
            self.resolve(arg, ToObject::new(is_receiving)).await
        } else {
            self.resolve(arg, ToPure).await
        }
    }

    async fn resolve_move_call_args(
        &mut self,
        package: MovePackage,
        module_name: &Identifier,
        function_name: &Identifier,
        ty_args: &[TypeTag],
        args: Vec<PTBArg>,
    ) -> PTBResult<Vec<Tx::Argument>> {
        let module = package
            .deserialize_module(module_name, VERSION_MAX, true)
            .map_err(|e| err!(self, "{e}"))?;
        let fdef = module
            .function_defs
            .iter()
            .find(|fdef| {
                module.identifier_at(module.function_handle_at(fdef.function).name)
                    == function_name.as_ident_str()
            })
            .ok_or_else(|| {
                err!(
                    self,
                    "Could not resolve function {} in module {}",
                    function_name,
                    module_name
                )
            })?;
        let function_signature = module.function_handle_at(fdef.function);
        let parameters = &module.signature_at(function_signature.parameters).0;
        let view = BinaryIndexedView::Module(&module);

        if parameters.len() != args.len() {
            error!(
                self,
                "Expected {} arguments, got {}",
                parameters.len(),
                args.len()
            );
        }

        let mut call_args = vec![];
        for (param, arg) in parameters.iter().zip(args.into_iter()) {
            let call_arg = self
                .resolve_move_call_arg(&view, ty_args, arg, param)
                .await?;
            call_args.push(call_arg);
        }
        Ok(call_args)
    }

    #[async_recursion]
    async fn resolve(
        &mut self,
        arg: PTBArg,
        mut ctx: impl Resolver<'a> + 'async_recursion,
    ) -> PTBResult<Tx::Argument> {
        match arg {
            PTBArg::Gas => Ok(Tx::Argument::GasCoin),
            PTBArg::Identifier(i) if self.resolved_arguments.contains_key(&i) => {
                Ok(self.resolved_arguments[&i].clone())
            }
            PTBArg::Identifier(i) if self.arguments_to_resolve.contains_key(&i) => {
                let arg = self.arguments_to_resolve[&i].clone();
                let resolved = self.resolve(arg, ctx).await?;
                self.arguments_to_resolve.remove(&i);
                self.resolved_arguments.insert(i, resolved.clone());
                Ok(resolved)
            }
            PTBArg::Bool(b) => ctx.pure(self, b).await,
            PTBArg::U8(u) => ctx.pure(self, u).await,
            PTBArg::U16(u) => ctx.pure(self, u).await,
            PTBArg::U32(u) => ctx.pure(self, u).await,
            PTBArg::U64(u) => ctx.pure(self, u).await,
            PTBArg::U128(u) => ctx.pure(self, u).await,
            PTBArg::U256(u) => ctx.pure(self, u).await,
            PTBArg::String(s) => ctx.pure(self, s).await,
            x @ PTBArg::Option(_) => {
                ctx.pure(
                    self,
                    x.into_move_value_opt().map_err(|e| err!(self, "{e}"))?,
                )
                .await
            }
            x @ PTBArg::Vector(_) => {
                ctx.pure(
                    self,
                    x.into_move_value_opt().map_err(|e| err!(self, "{e}"))?,
                )
                .await
            }
            PTBArg::Address(addr) => {
                let object_id = ObjectID::from_address(addr.into_inner());
                ctx.resolve_object_id(self, object_id).await
            }
            PTBArg::VariableAccess(head, fields) => {
                if fields.len() != 1 {
                    error!(
                        self,
                        "Tried to access the result {} more than one field: {:?}", head, fields,
                    );
                }
                match self.resolved_arguments.get(&head) {
                    Some(Tx::Argument::Result(u)) => Ok(Tx::Argument::NestedResult(*u, fields[0])),
                    Some(
                        x @ (Tx::Argument::NestedResult(..)
                        | Tx::Argument::Input(..)
                        | Tx::Argument::GasCoin),
                    ) => {
                        error!(
                            self,
                            "Tried to access a nested result, input, or gascoin {}: {}", head, x,
                        );
                    }
                    None => {
                        error!(self, "Tried to access an unresolved identifier: {:?}", head,);
                    }
                }
            }
            PTBArg::Identifier(i) => {
                error!(self, "unresolved identifier: {:?}", i);
            }
            PTBArg::Array(_) => {
                error!(self,"Tried to resolve array -- this is invalid and means that you nested an array inside a Move value (or another array)");
            }
            PTBArg::ModuleAccess { .. } => {
                error!(
                    self,
                    "Tried to resolve module access -- this shouldn't happen"
                );
            }
            PTBArg::TyArgs(..) => {
                error!(
                    self,
                    "Tried to resolve type arguments -- this shouldn't happen"
                );
            }
        }
    }

    async fn get_object(&self, object_id: ObjectID) -> PTBResult<SuiObjectData> {
        let res = self
            .reader
            .get_object_with_options(
                object_id,
                SuiObjectDataOptions::new().with_type().with_owner(),
            )
            .await
            .map_err(|e| err!(self, "{e}"))?
            .into_object()
            .map_err(|e| err!(self, "{e}"))?;
        Ok(res)
    }

    pub async fn handle_command(&mut self, mut command: ParsedPTBCommand<PTBArg>) -> PTBResult<()> {
        let tok = &command.name;
        match tok {
            CommandToken::TransferObjects => {
                assert!(command.args.len() == 2);
                let PTBArg::Array(obj_args) = command.args.pop().unwrap() else {
                    error!(self, "expected array of objects",);
                };
                let to_address = command.args.pop().unwrap();
                let to_arg = self.resolve(to_address, ToPure).await?;
                let mut transfer_args = vec![];
                for o in obj_args.into_iter() {
                    let arg = self.resolve(o, ToObject::default()).await?;
                    transfer_args.push(arg);
                }
                self.last_command = Some(
                    self.ptb
                        .command(Tx::Command::TransferObjects(transfer_args, to_arg)),
                );
            }
            CommandToken::Assign if command.args.len() == 1 => {
                let PTBArg::Identifier(i) = command.args.pop().unwrap() else {
                    error!(self, "expected identifier",);
                };
                let Some(prev_ptb_arg) = self.last_command.take() else {
                    error!(self, "Invalid assignment command",);
                };
                self.declare_identifier(i.clone());
                self.resolved_arguments.insert(i, prev_ptb_arg);
            }
            CommandToken::Assign if command.args.len() == 2 => {
                let arg = command.args.pop().unwrap();
                let PTBArg::Identifier(i) = command.args.pop().unwrap() else {
                    error!(self, "expected identifier",);
                };
                self.declare_identifier(i.clone());
                self.arguments_to_resolve.insert(i, arg);
            }
            CommandToken::Assign => error!(self, "expected 1 or 2 arguments for assignment",),
            CommandToken::MakeMoveVec => {
                let PTBArg::Array(args) = command.args.pop().unwrap() else {
                    error!(self, "expected array of argument",);
                };
                let PTBArg::TyArgs(ty_args) = command.args.pop().unwrap() else {
                    error!(self, "expected type argument",);
                };
                if ty_args.len() != 1 {
                    error!(self, "expected 1 type argumen",);
                }
                let ty_arg = ty_args[0]
                    .clone()
                    .into_type_tag(&resolve_address)
                    .map_err(|e| err!(self, "{e}"))?;
                let mut vec_args: Vec<Tx::Argument> = vec![];
                if is_pure(&ty_arg).map_err(|e| err!(self, "{e}"))? {
                    for arg in args.into_iter() {
                        let arg = self.resolve(arg, ToPure).await?;
                        vec_args.push(arg);
                    }
                } else {
                    for arg in args.into_iter() {
                        let arg = self.resolve(arg, ToObject::default()).await?;
                        vec_args.push(arg);
                    }
                }
                let res = self
                    .ptb
                    .command(Tx::Command::MakeMoveVec(Some(ty_arg), vec_args));
                self.last_command = Some(res);
            }
            CommandToken::SplitCoins => {
                if command.args.len() != 2 {
                    error!(self, "expected 2 argument",);
                }

                let PTBArg::Array(amounts) = command.args.pop().unwrap() else {
                    error!(self, "expected array of amount",);
                };

                let pre_coin = command.args.pop().unwrap();

                let coin = self.resolve(pre_coin, ToObject::default()).await?;
                let mut args = vec![];
                for arg in amounts.into_iter() {
                    let arg = self.resolve(arg, ToPure).await?;
                    args.push(arg);
                }
                let res = self
                    .ptb
                    .command(Tx::Command::SplitCoins(coin.clone(), args));
                self.last_command = Some(res);
            }
            CommandToken::MergeCoins => {
                if command.args.len() != 2 {
                    error!(self, "expected 2 argument",);
                }

                let PTBArg::Array(coins) = command.args.pop().unwrap() else {
                    error!(self, "expected array of coin",);
                };

                let pre_coin = command.args.pop().unwrap();

                let coin = self.resolve(pre_coin, ToObject::default()).await?;
                let mut args = vec![];
                for arg in coins.into_iter() {
                    let arg = self.resolve(arg, ToObject::default()).await?;
                    args.push(arg);
                }
                let res = self
                    .ptb
                    .command(Tx::Command::MergeCoins(coin.clone(), args));
                self.last_command = Some(res);
            }
            CommandToken::PickGasBudget => {
                let PTBArg::Identifier(i) = command.args.pop().unwrap() else {
                    error!(self, "expected identifie",);
                };
                let picker = match i.to_string().as_str() {
                    "max" => GasPicker::Max,
                    "min" => GasPicker::Min,
                    "sum" => GasPicker::Sum,
                    x => error!(self, "invalid gas picker: {}", x,),
                };
                self.gas_budget
                    .set_gas_picker(picker)
                    .map_err(|e| err!(self, "{e}"))?;
            }
            CommandToken::GasBudget => {
                let PTBArg::U64(budget) = command.args.pop().unwrap() else {
                    error!(self, "expected gas budget");
                };
                self.gas_budget.add_gas_budget(budget);
            }
            CommandToken::File => {
                error!(self, "File commands should be removed at this point");
            }
            CommandToken::FileStart => {
                assert!(command.args.len() == 1);
                let PTBArg::String(file_name) = command.args.pop().unwrap() else {
                    error!(self, "expected file name");
                };
                self.context.push_file_scope(file_name);
            }
            CommandToken::FileEnd => {
                assert!(command.args.len() == 1);
                let PTBArg::String(file_name) = command.args.pop().unwrap() else {
                    error!(self, "expected file name");
                };
                self.context.pop_file_scope(file_name)?;
            }
            CommandToken::MoveCall => {
                if command.args.len() > 3 || command.args.is_empty() {
                    error!(
                        self,
                        "expected less then or equal to 3 arguments for move call {}",
                        command.args.len()
                    );
                }
                let mut args = vec![];
                let mut ty_args = vec![];
                let PTBArg::ModuleAccess {
                    address,
                    module_name,
                    function_name,
                } = command.args.remove(0)
                else {
                    error!(self, "expected module access");
                };

                for arg in command.args.into_iter() {
                    if let PTBArg::TyArgs(targs) = arg {
                        for t in targs.into_iter() {
                            ty_args.push(
                                t.into_type_tag(&resolve_address)
                                    .map_err(|e| err!(self, "{e}"))?,
                            )
                        }
                    } else {
                        args.push(arg);
                    }
                }
                let package_id = ObjectID::from_address(address.into_inner());
                let package = self.resolve_to_package(package_id).await?;
                let args = self
                    .resolve_move_call_args(package, &module_name, &function_name, &ty_args, args)
                    .await?;
                let move_call = Tx::ProgrammableMoveCall {
                    package: package_id,
                    module: module_name,
                    function: function_name,
                    type_arguments: ty_args,
                    arguments: args,
                };
                let res = self.ptb.command(Tx::Command::MoveCall(Box::new(move_call)));
                self.last_command = Some(res);
            }
            CommandToken::Publish => {
                if command.args.len() != 1 {
                    error!(
                        self,
                        "expected 1 argument for publish but got {}",
                        command.args.len()
                    );
                }
                let PTBArg::String(package_path) = command.args.pop().unwrap() else {
                    error!(self, "expected filepath argument for publish",);
                };
                let package_path = std::path::PathBuf::from(package_path);
                let (dependencies, compiled_modules, _, _) = compile_package(
                    self.reader,
                    BuildConfig::default(),
                    package_path,
                    false, /* with_unpublished_dependencies */
                    false, /* skip_dependency_verification */
                )
                .await
                .map_err(|e| err!(self, "{e}"))?;

                let res = self.ptb.publish_upgradeable(
                    compiled_modules,
                    dependencies.published.into_values().collect(),
                );
                self.last_command = Some(res);
            }
            // Update this command to not do as many things. It should result in a single command.
            CommandToken::Upgrade => {
                if command.args.len() != 2 {
                    error!(
                        self,
                        "expected 2 arguments for upgrade but got {}",
                        command.args.len()
                    );
                }
                let mut arg = command.args.pop().unwrap();
                if let PTBArg::Identifier(id) = arg {
                    arg = self
                        .arguments_to_resolve
                        .get(&id)
                        .ok_or_else(
                            || err!(self, "Unable to find object ID argument for upgrade",),
                        )?
                        .clone();
                }
                let PTBArg::Address(upgrade_cap_id) = arg else {
                    error!(self, "expected upgrade cap object ID for upgrade",);
                };
                let PTBArg::String(package_path) = command.args.pop().unwrap() else {
                    error!(self, "expected filepath argument for publish",);
                };
                let package_path = std::path::PathBuf::from(package_path);

                let upgrade_cap_arg = self
                    .resolve(PTBArg::Address(upgrade_cap_id), ToObject::default())
                    .await?;

                let (package_id, compiled_modules, dependencies, package_digest, upgrade_policy) =
                    upgrade_package(
                        self.reader,
                        BuildConfig::default(),
                        package_path,
                        ObjectID::from_address(upgrade_cap_id.into_inner()),
                        false, /* with_unpublished_dependencies */
                        false, /* skip_dependency_verification */
                    )
                    .await
                    .map_err(|e| err!(self, "{e}"))?;

                let upgrade_arg = self
                    .ptb
                    .pure(upgrade_policy)
                    .map_err(|e| err!(self, "{e}"))?;
                let digest_arg = self
                    .ptb
                    .pure(package_digest)
                    .map_err(|e| err!(self, "{e}"))?;
                let upgrade_ticket =
                    self.ptb
                        .command(Tx::Command::MoveCall(Box::new(Tx::ProgrammableMoveCall {
                            package: SUI_FRAMEWORK_PACKAGE_ID,
                            module: ident_str!("package").to_owned(),
                            function: ident_str!("authorize_upgrade").to_owned(),
                            type_arguments: vec![],
                            arguments: vec![upgrade_cap_arg, upgrade_arg, digest_arg],
                        })));
                let upgrade_receipt = self.ptb.upgrade(
                    package_id,
                    upgrade_ticket,
                    dependencies.published.into_values().collect(),
                    compiled_modules,
                );
                let res =
                    self.ptb
                        .command(Tx::Command::MoveCall(Box::new(Tx::ProgrammableMoveCall {
                            package: SUI_FRAMEWORK_PACKAGE_ID,
                            module: ident_str!("package").to_owned(),
                            function: ident_str!("commit_upgrade").to_owned(),
                            type_arguments: vec![],
                            arguments: vec![upgrade_cap_arg, upgrade_receipt],
                        })));
                self.last_command = Some(res);
            }
            CommandToken::WarnShadows => {
                if command.args.len() == 1 {
                    self.warn_on_shadowing = true;
                } else {
                    error!(self, "expected no arguments for warn shadows");
                }
            }
            CommandToken::Preview => {
                if command.args.len() == 1 {
                    self.preview_set = true;
                } else {
                    error!(self, "expected no arguments for preview");
                }
            }
        }
        self.context.increment_file_command_index();
        Ok(())
    }
}
