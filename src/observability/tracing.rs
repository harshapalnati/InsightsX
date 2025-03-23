use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Initializes tracing with a Jaeger exporter.
/// This should be called at the start of your application.
pub fn init_tracing() {
    // Create a Jaeger exporter pipeline using agent
    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_service_name("insightsx")
        .install_simple()
        .expect("Failed to initialize Jaeger exporter");

    // Create the OpenTelemetry tracing layer
    let opentelemetry_layer = tracing_opentelemetry::layer();
    
    // Register the OpenTelemetry layer with the tracing subscriber
    tracing_subscriber::registry()
        .with(opentelemetry_layer)
        .init();
}