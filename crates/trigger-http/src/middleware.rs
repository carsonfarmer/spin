use anyhow::{Context, bail};
use wac_graph::{CompositionGraph, PackageId, types::Package};

use std::collections::HashMap;

use spin_factors_executor::{
    TriggerDependenciesComposer, TriggerDependency, TriggerDependencyData,
};

const HANDLER_PREFIX: &str = "wasi:http/handler@";
const HANDLER_INTERFACES: [&str; 2] = [
    "wasi:http/handler@0.3.0",
    "wasi:http/handler@0.3.0-rc-2026-03-15",
];

#[derive(Default)]
pub(crate) struct HttpMiddlewareComposer;

#[spin_core::async_trait]
impl TriggerDependenciesComposer for HttpMiddlewareComposer {
    async fn compose_trigger_dependencies(
        &self,
        trigger_dependencies: &HashMap<String, Vec<TriggerDependency>>,
        component: Vec<u8>,
    ) -> anyhow::Result<Vec<u8>> {
        let Some(middlewares) = trigger_dependencies.get("middleware") else {
            return Ok(component);
        };
        if trigger_dependencies.len() > 1 {
            bail!("the HTTP trigger's only allowed trigger dependency is `middleware`");
        }
        if middlewares.is_empty() {
            return Ok(component);
        }

        compose_middlewares(component, middlewares).await
    }
}

/// Chain a list of component packages into a middleware pipeline.
///
/// `packages` is ordered from **outermost** (first to receive a request) to
/// **innermost** (the final handler).  Every component except the last must
/// import a name equal to `import_name` and every component must export a name
/// equal to `export_name`.  In the common middleware pattern these are the same
/// (e.g. both `"handle"`), but they can differ if the WIT uses separate names.
///
/// Returns the [`NodeId`] of the alias for the outermost component's export,
/// ready to be passed to [`CompositionGraph::export`].
///
/// # Errors
///
/// Returns an error if fewer than two packages are provided, or if any
/// alias / argument wiring step fails.
fn chain(
    graph: &mut CompositionGraph,
    packages: &[PackageId],
    import_name: &str,
    export_name: &str,
) -> anyhow::Result<wac_graph::NodeId> {
    if packages.len() < 2 {
        bail!("chain requires at least 2 packages, got {}", packages.len());
    }

    // Start from the innermost component (last in the list) and work outward.
    // The innermost component is instantiated first with no wiring — its
    // unsatisfied imports (if any) will become implicit imports of the
    // composed component.
    let mut iter = packages.iter().rev();
    let innermost = *iter.next().unwrap();
    let mut instance = graph.instantiate(innermost);
    let mut upstream_handle = graph.alias_instance_export(instance, export_name)?;

    // For each remaining component (moving outward), instantiate it and
    // wire the previous component's export into its import.
    for &pkg in iter {
        instance = graph.instantiate(pkg);
        graph.set_instantiation_argument(instance, import_name, upstream_handle)?;
        upstream_handle = graph.alias_instance_export(instance, export_name)?;
    }

    Ok(upstream_handle)
}

fn handler_interfaces<'a>(names: impl Iterator<Item = &'a String>) -> Vec<&'a str> {
    names
        .map(String::as_str)
        .filter(|name| name.starts_with(HANDLER_PREFIX))
        .collect()
}

fn select_handler<'a>(names: impl Iterator<Item = &'a String>) -> anyhow::Result<&'a str> {
    let handlers = handler_interfaces(names);
    let [handler] = handlers.as_slice() else {
        bail!("component must expose exactly one wasi:http handler interface");
    };
    if !HANDLER_INTERFACES.contains(handler) {
        bail!("unsupported wasi:http handler interface `{handler}`");
    }
    Ok(handler)
}

async fn compose_middlewares(
    primary: Vec<u8>,
    middleware_blobs: &[TriggerDependency],
) -> anyhow::Result<Vec<u8>> {
    use spin_compose::DependencyLike;

    let mut graph = CompositionGraph::new();
    let mut package_ids: Vec<PackageId> = Vec::new();
    let primary = Package::from_bytes("primary", None, primary, graph.types_mut())
        .context("parsing primary component")?;
    let handler = select_handler(graph.types()[primary.ty()].exports.keys())
        .context("selecting primary handler")?
        .to_owned();

    // Register middleware packages (outermost → innermost order).
    for (index, dep) in middleware_blobs.iter().enumerate() {
        let bytes: Vec<u8> = match &dep.data {
            TriggerDependencyData::InMemory(data) => data.clone(),
            TriggerDependencyData::OnDisk(path) => tokio::fs::read(path)
                .await
                .with_context(|| format!("reading middleware from {}", path.display()))?,
        };
        let bytes = spin_componentize::componentize_if_necessary(&bytes)
            .context("failed to componentize")?;
        let bytes = spin_capabilities::apply_deny_adapter(&bytes, dep.dependency.inherit())?;
        let name = format!("middleware{index}");
        let package = Package::from_bytes(&name, None, bytes, graph.types_mut())
            .context("parsing middleware component")?;
        let world = &graph.types()[package.ty()];
        let imports = handler_interfaces(world.imports.keys());
        let exports = handler_interfaces(world.exports.keys());
        if imports.as_slice() != [handler.as_str()] || exports.as_slice() != [handler.as_str()] {
            bail!("middleware{index} must import and export `{handler}` exclusively");
        }
        package_ids.push(graph.register_package(package)?);
    }

    // Register the primary component (innermost in the chain).
    package_ids.push(graph.register_package(primary)?);

    // Wire the pipeline: outermost middleware → … → primary.
    let outermost_export = chain(&mut graph, &package_ids, &handler, &handler)?;

    // Export the outermost handler as the composed component's export.
    graph.export(outermost_export, &handler)?;

    Ok(graph.encode(Default::default())?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use spin_app::locked::{
        ContentRef, InheritConfiguration, LockedComponentDependency, LockedComponentSource,
    };
    use spin_factors_executor::TriggerDependencyData;
    use wit_parser::{LiftLowerAbi, ManglingAndAbi};

    #[tokio::test]
    async fn composes_stable_handler_chain() {
        compose_middlewares(
            component("0.3.0", "service"),
            &[dependency(component("0.3.0", "middleware"))],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn composes_release_candidate_handler_chain() {
        compose_middlewares(
            component("0.3.0-rc-2026-03-15", "service"),
            &[dependency(component("0.3.0-rc-2026-03-15", "middleware"))],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn rejects_mixed_handler_chains() {
        for (primary, middleware) in [
            ("0.3.0", "0.3.0-rc-2026-03-15"),
            ("0.3.0-rc-2026-03-15", "0.3.0"),
        ] {
            let error = compose_middlewares(
                component(primary, "service"),
                &[dependency(component(middleware, "middleware"))],
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains("must import and export"));
        }
    }

    #[test]
    fn rejects_ambiguous_handler_interfaces() {
        let names = [
            "wasi:http/handler@0.3.0".to_owned(),
            "wasi:http/handler@0.4.0".to_owned(),
        ];
        assert!(select_handler(names.iter()).is_err());
    }

    fn dependency(data: Vec<u8>) -> TriggerDependency {
        TriggerDependency {
            data: TriggerDependencyData::InMemory(data),
            dependency: LockedComponentDependency {
                source: LockedComponentSource {
                    content_type: "application/wasm".into(),
                    content: ContentRef::default(),
                },
                export: None,
                inherit: InheritConfiguration::All,
            },
        }
    }

    fn component(version: &str, world: &str) -> Vec<u8> {
        let wit = format!(
            "package wasi:http@{version};
             interface handler {{ handle: func(); }}
             world service {{ export handler; }}
             world middleware {{ import handler; export handler; }}"
        );
        let mut resolve = wit_parser::Resolve::default();
        let package = resolve.push_str("test", &wit).unwrap();
        let world = resolve.select_world(&[package], Some(world)).unwrap();
        let mut wasm = wit_component::dummy_module(
            &resolve,
            world,
            ManglingAndAbi::Legacy(LiftLowerAbi::Sync),
        );
        wit_component::embed_component_metadata(
            &mut wasm,
            &resolve,
            world,
            wit_component::StringEncoding::UTF8,
        )
        .unwrap();
        wit_component::ComponentEncoder::default()
            .validate(true)
            .module(&wasm)
            .unwrap()
            .encode()
            .unwrap()
    }
}
