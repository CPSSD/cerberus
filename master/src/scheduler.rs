use errors::*;

#[derive(Default)]
pub struct MapReduceScheduler {
    available_workers: u32,
}

impl MapReduceScheduler {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn schedule_mapreduce(&self) -> Result<String> {
        Ok("Success!".to_owned())
    }

    pub fn get_available_workers(&self) -> u32 {
        self.available_workers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_mapreduce() {
        let scheduler = MapReduceScheduler::new();
        assert_eq!("Success!", scheduler.schedule_mapreduce().unwrap());
    }
}
