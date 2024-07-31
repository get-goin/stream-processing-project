pub struct PipelineBuilder {
    pipeline: Pipeline,
}

impl PipelineBuilder {
    pub fn new(name: String) -> Self {
        PipelineBuilder {
            pipeline: Pipeline {
                id: Uuid::new_v4(),
                name,
                description: None,
                version: 1,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                stages: Vec::new(),
                sources: Vec::new(),
                sinks: Vec::new(),
                metadata: HashMap::new(),
            },
        }
    }

    pub fn add_source(mut self, source: Source) -> Self {
        self.pipeline.sources.push(source);
        self
    }

    pub fn add_stage(mut self, stage: Stage) -> Self {
        self.pipeline.stages.push(stage);
        self
    }

    pub fn add_sink(mut self, sink: Sink) -> Self {
        self.pipeline.sinks.push(sink);
        self
    }

    pub fn build(self) -> Result<Pipeline, PipelineValidationError> {
        // Perform validation
        self.validate()?;
        Ok(self.pipeline)
    }

    fn validate(&self) -> Result<(), PipelineValidationError> {
        // Implement validation logic
        // Check for cycles, ensure all inputs are connected, etc.
        Ok(())
    }
}