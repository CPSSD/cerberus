use errors::*;

struct MapReduceScheduler {
    available_workers: u32,
}

impl MapReduceScheduler {
    pub fn new() -> Self {
        MapReduceScheduler { available_workers: 0 }
    }

    pub fn schedule_mapreduce(&self) -> Result<String> {
        Ok("Success!".to_owned())
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
