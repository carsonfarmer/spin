use std::time::{Duration, Instant};
use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use spin_app::{App, AppComponent};
use spin_core::{Component, async_trait, wasmtime::CallHook};
use spin_factors::{
    AsInstanceState, ConfiguredApp, Factor, HasInstanceBuilder, RuntimeFactors,
    RuntimeFactorsInstanceState,
};

/// The observable outcome of a completed store.
pub enum StoreCompletionOutcome<'a> {
    /// The store returned successfully.
    Returned,
    /// The store failed.
    Failed(&'a wasmtime::Error),
    /// The store was dropped without an explicit outcome.
    Dropped,
}

/// Runtime facts observed when a store completes.
pub struct StoreCompletion<'a> {
    /// The component executed by the store.
    pub component_id: &'a str,
    /// Fuel available before execution, if fuel is enabled.
    pub initial_fuel: Option<u64>,
    /// Fuel remaining after execution, if fuel is enabled.
    pub remaining_fuel: Option<u64>,
    /// Time spent actively executing guest code.
    pub guest_active: Duration,
    /// Time elapsed since the store was created.
    pub wall: Duration,
    /// The observable store outcome.
    pub outcome: StoreCompletionOutcome<'a>,
}

type StoreCompletionObserver = Box<dyn for<'a> Fn(StoreCompletion<'a>) + Send + Sync + 'static>;

/// A FactorsExecutor manages execution of a Spin app.
///
/// It is generic over the executor's [`RuntimeFactors`]. Additionally, it
/// holds any other per-instance state needed by the caller.
pub struct FactorsExecutor<T: RuntimeFactors, U: 'static = ()> {
    core_engine: spin_core::Engine<InstanceState<T::InstanceState, U>>,
    factors: T,
    hooks: Vec<Box<dyn ExecutorHooks<T, U>>>,
}

impl<T: RuntimeFactors, U: Send + 'static> FactorsExecutor<T, U> {
    /// Constructs a new executor.
    pub fn new(
        mut core_engine_builder: spin_core::EngineBuilder<
            InstanceState<<T as RuntimeFactors>::InstanceState, U>,
        >,
        mut factors: T,
    ) -> anyhow::Result<Self> {
        factors
            .init(core_engine_builder.linker())
            .context("failed to initialize factors")?;
        Ok(Self {
            factors,
            core_engine: core_engine_builder.build(),
            hooks: Default::default(),
        })
    }

    pub fn core_engine(&self) -> &spin_core::Engine<InstanceState<T::InstanceState, U>> {
        &self.core_engine
    }

    // Adds the given [`ExecutorHooks`] to this executor.
    ///
    /// Hooks are run in the order they are added.
    pub fn add_hooks(&mut self, hooks: impl ExecutorHooks<T, U> + 'static) {
        self.hooks.push(Box::new(hooks));
    }

    /// Loads a [`App`] with this executor.
    pub async fn load_app(
        self: Arc<Self>,
        app: App,
        runtime_config: T::RuntimeConfig,
        component_loader: &impl ComponentLoader<T, U>,
        trigger_type: Option<&str>,
        trigger_dependencies_composer: impl TriggerDependenciesComposer,
    ) -> anyhow::Result<FactorsExecutorApp<T, U>> {
        let configured_app = self
            .factors
            .configure_app(app, runtime_config)
            .context("failed to configure app")?;

        for hooks in &self.hooks {
            hooks.configure_app(&configured_app).await?;
        }

        let components = match trigger_type {
            Some(trigger_type) => configured_app
                .app()
                .triggers_with_type(trigger_type)
                .filter_map(|t| t.component().ok())
                .collect::<Vec<_>>(),
            None => configured_app.app().components().collect(),
        };
        let mut component_instance_pres = HashMap::with_capacity(components.len());

        for component in components {
            let instance_pre = component_loader
                .load_instance_pre(
                    &self.core_engine,
                    &component,
                    &trigger_dependencies_composer,
                )
                .await?;
            component_instance_pres.insert(component.id().to_string(), instance_pre);
        }

        Ok(FactorsExecutorApp {
            executor: self.clone(),
            configured_app,
            component_instance_pres,
        })
    }
}

#[async_trait]
pub trait ExecutorHooks<T, U>: Send + Sync
where
    T: RuntimeFactors,
{
    /// Configure app hooks run immediately after [`RuntimeFactors::configure_app`].
    async fn configure_app(&self, configured_app: &ConfiguredApp<T>) -> anyhow::Result<()> {
        let _ = configured_app;
        Ok(())
    }

    /// Prepare instance hooks run immediately before [`FactorsExecutorApp::prepare`] returns.
    fn prepare_instance(&self, builder: &mut FactorsInstanceBuilder<T, U>) -> anyhow::Result<()> {
        let _ = builder;
        Ok(())
    }
}

/// A ComponentLoader is responsible for loading Wasmtime [`Component`]s.
#[async_trait]
pub trait ComponentLoader<T: RuntimeFactors, U>: Sync {
    /// Loads a [`Component`] for the given [`AppComponent`].
    async fn load_component(
        &self,
        engine: &spin_core::wasmtime::Engine,
        component: &AppComponent,
        trigger_dependencies_composer: &impl TriggerDependenciesComposer,
    ) -> anyhow::Result<Component>;

    /// Loads [`InstancePre`] for the given [`AppComponent`].
    async fn load_instance_pre(
        &self,
        engine: &spin_core::Engine<InstanceState<T::InstanceState, U>>,
        component: &AppComponent,
        trigger_dependencies_composer: &impl TriggerDependenciesComposer,
    ) -> anyhow::Result<spin_core::InstancePre<InstanceState<T::InstanceState, U>>> {
        let component = self
            .load_component(engine.as_ref(), component, trigger_dependencies_composer)
            .await?;
        engine.instantiate_pre(&component)
    }
}

#[async_trait]
pub trait TriggerDependenciesComposer: Send + Sync {
    async fn compose_trigger_dependencies(
        &self,
        trigger_dependencies: &HashMap<String, Vec<TriggerDependency>>,
        component: Vec<u8>,
    ) -> anyhow::Result<Vec<u8>>;
}

#[async_trait]
impl TriggerDependenciesComposer for () {
    async fn compose_trigger_dependencies(
        &self,
        trigger_dependencies: &HashMap<String, Vec<TriggerDependency>>,
        component: Vec<u8>,
    ) -> anyhow::Result<Vec<u8>> {
        if trigger_dependencies.is_empty() {
            Ok(component)
        } else {
            Err(anyhow::anyhow!("this trigger should not have dependencies"))
        }
    }
}

pub struct TriggerDependency {
    pub data: TriggerDependencyData,
    pub dependency: spin_app::locked::LockedComponentDependency,
}

pub enum TriggerDependencyData {
    InMemory(Vec<u8>),
    OnDisk(std::path::PathBuf),
}

type InstancePre<T, U> =
    spin_core::InstancePre<InstanceState<<T as RuntimeFactors>::InstanceState, U>>;

/// A FactorsExecutorApp represents a loaded Spin app, ready for instantiation.
///
/// It is generic over the executor's [`RuntimeFactors`] and any ad-hoc additional
/// per-instance state needed by the caller.
pub struct FactorsExecutorApp<T: RuntimeFactors, U: 'static> {
    executor: Arc<FactorsExecutor<T, U>>,
    configured_app: ConfiguredApp<T>,
    // Maps component IDs -> InstancePres
    component_instance_pres: HashMap<String, InstancePre<T, U>>,
}

impl<T: RuntimeFactors, U: Send + 'static> FactorsExecutorApp<T, U> {
    pub fn engine(&self) -> &spin_core::Engine<InstanceState<T::InstanceState, U>> {
        &self.executor.core_engine
    }

    pub fn configured_app(&self) -> &ConfiguredApp<T> {
        &self.configured_app
    }

    pub fn app(&self) -> &App {
        self.configured_app.app()
    }

    pub fn get_component(&self, component_id: &str) -> anyhow::Result<&Component> {
        Ok(self.get_instance_pre(component_id)?.component())
    }

    pub fn get_instance_pre(&self, component_id: &str) -> anyhow::Result<&InstancePre<T, U>> {
        self.component_instance_pres
            .get(component_id)
            .with_context(|| format!("no such component {component_id:?}"))
    }

    /// Returns an instance builder for the given component ID.
    pub fn prepare(&self, component_id: &str) -> anyhow::Result<FactorsInstanceBuilder<'_, T, U>> {
        let app_component = self
            .configured_app
            .app()
            .get_component(component_id)
            .with_context(|| format!("no such component {component_id:?}"))?;

        let instance_pre = self.component_instance_pres.get(component_id).unwrap();

        let factor_builders = self
            .executor
            .factors
            .prepare(&self.configured_app, component_id)?;

        let store_builder = self.executor.core_engine.store_builder();

        let mut builder = FactorsInstanceBuilder {
            store_builder,
            factor_builders,
            instance_pre,
            app_component,
            factors: &self.executor.factors,
            completion_observer: None,
        };

        for hooks in &self.executor.hooks {
            hooks.prepare_instance(&mut builder)?;
        }

        Ok(builder)
    }
}

/// A FactorsInstanceBuilder manages the instantiation of a Spin component instance.
///
/// It is generic over the executor's [`RuntimeFactors`] and any ad-hoc additional
/// per-instance state needed by the caller.
pub struct FactorsInstanceBuilder<'a, F: RuntimeFactors, U: 'static> {
    app_component: AppComponent<'a>,
    store_builder: spin_core::StoreBuilder,
    factor_builders: F::InstanceBuilders,
    instance_pre: &'a InstancePre<F, U>,
    factors: &'a F,
    completion_observer: Option<StoreCompletionObserver>,
}

impl<T: RuntimeFactors, U: 'static> FactorsInstanceBuilder<'_, T, U> {
    /// Returns the app component for the instance.
    pub fn app_component(&self) -> &AppComponent<'_> {
        &self.app_component
    }

    /// Returns the store builder for the instance.
    pub fn store_builder(&mut self) -> &mut spin_core::StoreBuilder {
        &mut self.store_builder
    }

    /// Returns the factor instance builders for the instance.
    pub fn factor_builders(&mut self) -> &mut T::InstanceBuilders {
        &mut self.factor_builders
    }

    /// Returns the specific instance builder for the given factor.
    pub fn factor_builder<F: Factor>(&mut self) -> Option<&mut F::InstanceBuilder> {
        self.factor_builders().for_factor::<F>()
    }

    /// Returns the underlying wasmtime engine for the instance.
    pub fn wasmtime_engine(&self) -> &spin_core::WasmtimeEngine {
        self.instance_pre.engine()
    }

    /// Returns the compiled component for the instance.
    pub fn component(&self) -> &Component {
        self.instance_pre.component()
    }

    /// Observes completion synchronously; the observer must not block or panic.
    pub fn on_store_completion(
        &mut self,
        observer: impl for<'a> Fn(StoreCompletion<'a>) + Send + Sync + 'static,
    ) {
        self.completion_observer = Some(Box::new(observer));
    }
}

impl<T: RuntimeFactors, U: Send> FactorsInstanceBuilder<'_, T, U> {
    /// Instantiates the instance with the given executor instance state
    pub async fn instantiate(
        self,
        executor_instance_state: U,
    ) -> anyhow::Result<(
        spin_core::Instance,
        spin_core::Store<InstanceState<T::InstanceState, U>>,
    )> {
        let instance_pre = self.instance_pre;
        let mut store = self.build_store(executor_instance_state)?;
        let instance = instance_pre.instantiate_async(&mut store).await?;

        // Track memory usage after instantiation in the instance state.
        // Note: This only applies if the component has initial memory reservations.
        store.data_mut().memory_used_on_init = store.data().core_state().memory_consumed();

        Ok((instance, store))
    }

    fn build_store(
        self,
        executor_instance_state: U,
    ) -> anyhow::Result<spin_core::Store<InstanceState<T::InstanceState, U>>> {
        let instance_state = InstanceState {
            core: Default::default(),
            factors: self.factors.build_instance_state(self.factor_builders)?,
            executor: executor_instance_state,
            cpu_time_elapsed: Duration::from_millis(0),
            cpu_time_last_entry: None,
            memory_used_on_init: 0,
            component_id: self.app_component.id().into(),
            started_at: self.completion_observer.as_ref().map(|_| Instant::now()),
            initial_fuel: None,
            remaining_fuel: None,
            completion_observer: self.completion_observer,
        };
        let mut store = self.store_builder.build(instance_state)?;
        let initial_fuel = store.as_mut().get_fuel().ok();
        store.data_mut().initial_fuel = initial_fuel;
        store.data_mut().remaining_fuel = initial_fuel;

        if cfg!(feature = "cpu-time-metrics") || store.data().completion_observer.is_some() {
            store.as_mut().call_hook(|mut store, hook| {
                store.data_mut().remaining_fuel = store.get_fuel().ok();
                CpuTimeCallHook.handle_call_event::<T, U>(store.data_mut(), hook)
            });
        }
        Ok(store)
    }

    pub fn instantiate_store(
        self,
        executor_instance_state: U,
    ) -> anyhow::Result<spin_core::Store<InstanceState<T::InstanceState, U>>> {
        self.build_store(executor_instance_state)
    }
}

/// Completes a store with an explicit outcome.
pub fn complete_store<T: 'static, U: 'static>(
    mut store: impl wasmtime::AsContextMut<Data = InstanceState<T, U>>,
    result: Result<(), &wasmtime::Error>,
) {
    let remaining_fuel = store.as_context_mut().get_fuel().ok();
    let outcome = result.map_or_else(StoreCompletionOutcome::Failed, |_| {
        StoreCompletionOutcome::Returned
    });
    store
        .as_context_mut()
        .data_mut()
        .complete(remaining_fuel, outcome);
}

// Tracks CPU time used by a Wasm guest.
struct CpuTimeCallHook;

impl CpuTimeCallHook {
    fn handle_call_event<T: RuntimeFactors, U>(
        &self,
        state: &mut InstanceState<T::InstanceState, U>,
        ch: CallHook,
    ) -> wasmtime::Result<()> {
        match ch {
            CallHook::CallingWasm | CallHook::ReturningFromHost => {
                debug_assert!(state.cpu_time_last_entry.is_none());
                state.cpu_time_last_entry = Some(Instant::now());
            }
            CallHook::ReturningFromWasm | CallHook::CallingHost => {
                let elapsed = state.cpu_time_last_entry.take().unwrap().elapsed();
                state.cpu_time_elapsed += elapsed;
            }
        }

        Ok(())
    }
}

/// InstanceState is the [`spin_core::Store`] `data` for an instance.
///
/// It is generic over the [`RuntimeFactors::InstanceState`] and any ad-hoc
/// data needed by the caller.
pub struct InstanceState<T, U> {
    core: spin_core::State,
    factors: T,
    executor: U,
    /// The component ID.
    component_id: String,

    /// The last time guest code started running in this instance.
    cpu_time_last_entry: Option<Instant>,
    /// The total CPU time elapsed actively running guest code in this instance.
    cpu_time_elapsed: Duration,
    /// The memory (in bytes) consumed on initialization.
    memory_used_on_init: u64,
    started_at: Option<Instant>,
    initial_fuel: Option<u64>,
    remaining_fuel: Option<u64>,
    completion_observer: Option<StoreCompletionObserver>,
}

impl<T, U> Drop for InstanceState<T, U> {
    fn drop(&mut self) {
        if self.cpu_time_last_entry.is_some() {
            self.remaining_fuel = None;
        }
        self.complete(self.remaining_fuel, StoreCompletionOutcome::Dropped);

        // Record the component execution time.
        #[cfg(feature = "cpu-time-metrics")]
        spin_telemetry::metrics::histogram!(
            spin.component_cpu_time = self.cpu_time_elapsed.as_secs_f64(),
            component_id = self.component_id,
            // According to the OpenTelemetry spec, instruments measuring durations should use "s" as the unit.
            // See https://opentelemetry.io/docs/specs/semconv/general/metrics/#units
            unit = "s"
        );

        // Record the component memory consumed on initialization.
        spin_telemetry::metrics::histogram!(
            spin.component_memory_used_on_init = self.memory_used_on_init,
            component_id = self.component_id,
            unit = "By"
        );

        // Record the component memory consumed during execution.
        spin_telemetry::metrics::histogram!(
            spin.component_memory_used = self.core.memory_consumed(),
            component_id = self.component_id,
            unit = "By"
        );
    }
}

impl<T, U> InstanceState<T, U> {
    fn complete(&mut self, remaining_fuel: Option<u64>, outcome: StoreCompletionOutcome<'_>) {
        let Some(observer) = self.completion_observer.take() else {
            return;
        };
        if let Some(started) = self.cpu_time_last_entry.take() {
            self.cpu_time_elapsed += started.elapsed();
        }
        observer(StoreCompletion {
            component_id: &self.component_id,
            initial_fuel: self.initial_fuel,
            remaining_fuel,
            guest_active: self.cpu_time_elapsed,
            wall: self.started_at.unwrap().elapsed(),
            outcome,
        });
    }

    /// Provides access to the [`spin_core::State`].
    pub fn core_state(&self) -> &spin_core::State {
        &self.core
    }

    /// Provides mutable access to the [`spin_core::State`].
    pub fn core_state_mut(&mut self) -> &mut spin_core::State {
        &mut self.core
    }

    /// Provides access to the [`RuntimeFactors::InstanceState`].
    pub fn factors_instance_state(&self) -> &T {
        &self.factors
    }

    /// Provides mutable access to the [`RuntimeFactors::InstanceState`].
    pub fn factors_instance_state_mut(&mut self) -> &mut T {
        &mut self.factors
    }

    /// Provides access to the ad-hoc executor instance state.
    pub fn executor_instance_state(&self) -> &U {
        &self.executor
    }

    /// Provides mutable access to the ad-hoc executor instance state.
    pub fn executor_instance_state_mut(&mut self) -> &mut U {
        &mut self.executor
    }
}

impl<T, U> spin_core::AsState for InstanceState<T, U> {
    fn as_state(&mut self) -> &mut spin_core::State {
        &mut self.core
    }
}

impl<T: RuntimeFactorsInstanceState, U> AsInstanceState<T> for InstanceState<T, U> {
    fn as_instance_state(&mut self) -> &mut T {
        &mut self.factors
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use spin_factor_wasi::{DummyFilesMounter, WasiFactor};
    use spin_factors::RuntimeFactors;
    use spin_factors_test::TestEnvironment;

    use super::*;

    #[derive(RuntimeFactors)]
    struct TestFactors {
        wasi: WasiFactor,
    }

    #[tokio::test]
    async fn instance_builder_works() -> anyhow::Result<()> {
        let factors = TestFactors {
            wasi: WasiFactor::new(DummyFilesMounter),
        };
        let env = TestEnvironment::new(factors);
        let locked = env.build_locked_app().await?;
        let app = App::new("test-app", locked);

        let mut config = spin_core::Config::default();
        config.wasmtime_config().consume_fuel(true);
        let engine_builder = spin_core::Engine::builder(&config)?;
        let executor = Arc::new(FactorsExecutor::new(engine_builder, env.factors)?);

        let factors_app = executor
            .load_app(app, Default::default(), &DummyComponentLoader, None, ())
            .await?;

        let mut instance_builder = factors_app.prepare("empty")?;

        assert_eq!(instance_builder.app_component().id(), "empty");

        instance_builder.store_builder().max_memory_size(1_000_000);

        instance_builder
            .factor_builder::<WasiFactor>()
            .unwrap()
            .args(["foo"]);

        let observations = Arc::new(Mutex::new(Vec::new()));
        instance_builder.on_store_completion(observer(observations.clone()));

        let (instance, mut store) = instance_builder.instantiate(()).await?;
        store.as_mut().set_fuel(100)?;
        store.data_mut().initial_fuel = Some(100);
        store.data_mut().remaining_fuel = Some(100);
        store.data_mut().cpu_time_elapsed = Duration::ZERO;
        let run = instance.get_typed_func::<(), ()>(&mut store, "run")?;
        run.call_async(&mut store, ()).await?;
        complete_store(&mut store, Ok(()));
        drop(store);

        let observations = observations.lock().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].component_id, "empty");
        assert_eq!(observations[0].outcome, ObservedOutcome::Returned);
        assert!(observations[0].remaining_fuel < Some(100));
        assert!(observations[0].guest_active > Duration::ZERO);
        Ok(())
    }

    #[test]
    fn completion_outcomes_are_observed_exactly_once() -> anyhow::Result<()> {
        let observations = Arc::new(Mutex::new(Vec::new()));

        let mut returned = test_store(observations.clone())?;
        complete_store(&mut returned, Ok(()));
        complete_store(&mut returned, Ok(()));
        drop(returned);

        let mut failed = test_store(observations.clone())?;
        let error = wasmtime::Error::msg("boom");
        complete_store(&mut failed, Err(&error));
        drop(failed);

        drop(test_store(observations.clone())?);

        let observations = observations.lock().unwrap();
        assert_eq!(observations.len(), 3);
        assert_eq!(observations[0].outcome, ObservedOutcome::Returned);
        assert_eq!(
            observations[1].outcome,
            ObservedOutcome::Failed("boom".into())
        );
        assert_eq!(observations[2].outcome, ObservedOutcome::Dropped);
        assert!(observations.iter().all(|o| o.initial_fuel == Some(100)));
        assert!(observations.iter().all(|o| o.remaining_fuel == Some(80)));
        Ok(())
    }

    #[test]
    fn active_drop_does_not_report_stale_remaining_fuel() -> anyhow::Result<()> {
        let observations = Arc::new(Mutex::new(Vec::new()));
        let mut store = test_store(observations.clone())?;
        store.data_mut().cpu_time_last_entry = Some(Instant::now());
        drop(store);

        let observations = observations.lock().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].outcome, ObservedOutcome::Dropped);
        assert_eq!(observations[0].remaining_fuel, None);
        Ok(())
    }

    #[derive(Debug, PartialEq)]
    enum ObservedOutcome {
        Returned,
        Failed(String),
        Dropped,
    }

    struct Observed {
        component_id: String,
        initial_fuel: Option<u64>,
        remaining_fuel: Option<u64>,
        guest_active: Duration,
        outcome: ObservedOutcome,
    }

    fn observer(observations: Arc<Mutex<Vec<Observed>>>) -> StoreCompletionObserver {
        Box::new(move |completion| {
            let outcome = match completion.outcome {
                StoreCompletionOutcome::Returned => ObservedOutcome::Returned,
                StoreCompletionOutcome::Failed(error) => ObservedOutcome::Failed(error.to_string()),
                StoreCompletionOutcome::Dropped => ObservedOutcome::Dropped,
            };
            observations.lock().unwrap().push(Observed {
                component_id: completion.component_id.into(),
                initial_fuel: completion.initial_fuel,
                remaining_fuel: completion.remaining_fuel,
                guest_active: completion.guest_active,
                outcome,
            });
        })
    }

    fn test_store(
        observations: Arc<Mutex<Vec<Observed>>>,
    ) -> anyhow::Result<spin_core::Store<InstanceState<(), ()>>> {
        let mut config = spin_core::Config::default();
        config.wasmtime_config().consume_fuel(true);
        let engine: spin_core::Engine<InstanceState<(), ()>> =
            spin_core::Engine::builder(&config)?.build();
        let state = InstanceState {
            core: Default::default(),
            factors: (),
            executor: (),
            component_id: "test".into(),
            cpu_time_last_entry: None,
            cpu_time_elapsed: Duration::ZERO,
            memory_used_on_init: 0,
            started_at: Some(Instant::now()),
            initial_fuel: Some(100),
            remaining_fuel: Some(80),
            completion_observer: Some(observer(observations)),
        };
        let mut store = engine.store_builder().build(state)?;
        store.as_mut().set_fuel(80)?;
        Ok(store)
    }

    struct DummyComponentLoader;

    #[async_trait]
    impl ComponentLoader<TestFactors, ()> for DummyComponentLoader {
        async fn load_component(
            &self,
            engine: &spin_core::wasmtime::Engine,
            _component: &AppComponent,
            _trigger_dependencies_composer: &impl TriggerDependenciesComposer,
        ) -> anyhow::Result<Component> {
            Ok(Component::new(
                engine,
                r#"
                    (component
                        (core module $module
                            (func (export "run")
                                i32.const 1
                                i32.const 2
                                i32.add
                                drop
                            )
                        )
                        (core instance $instance (instantiate $module))
                        (func (export "run")
                            (canon lift (core func $instance "run"))
                        )
                    )
                "#,
            )?)
        }
    }
}
