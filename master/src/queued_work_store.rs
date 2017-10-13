use errors::*;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::hash::Hash;

/// The `QueuedWork` trait defines a an object that can be stored in a `QueuedWorkStore`.
pub trait QueuedWork {
    type Key: Hash + Eq + Default;
    fn get_work_bucket(&self) -> Self::Key;
    fn get_work_id(&self) -> Self::Key;
}

/// The `QueuedWorkStore` is a struct that owns and stores objects that implement the `QueuedWork`
/// trait.
#[derive(Default)]
pub struct QueuedWorkStore<T>
where
    T: QueuedWork + Default,
{
    // A map of TaskId -> Queued_Work.
    work_map: HashMap<T::Key, Box<T>>,
    work_buckets: HashMap<T::Key, Vec<T::Key>>,
    // TODO(Conor): Consider changing this to a priority queue or a LinkedList.
    // If a priority is not required, changing this to a LinkedList will improve performance.
    work_queue: VecDeque<T::Key>,
}

impl<T> QueuedWorkStore<T>
where
    T: QueuedWork + Default,
{
    pub fn new() -> Self {
        Default::default()
    }

    pub fn add_to_store(&mut self, task: Box<T>) -> Result<()> {
        if self.work_map.contains_key(&task.get_work_id()) {
            return Err("Given task is already in the store".into());
        }
        // Add task to work_bucket vector.
        let work_bucket: T::Key = task.get_work_bucket();
        let bucket: &mut Vec<T::Key> = self.work_buckets.entry(work_bucket).or_insert_with(
            Vec::new,
        );
        bucket.push(task.get_work_id());

        // Add task to queue.
        self.work_queue.push_back(task.get_work_id());

        // Add task to work map.
        self.work_map.insert(task.get_work_id(), task);

        Ok(())
    }

    pub fn remove_task(&mut self, task_id: &T::Key) -> Result<()> {
        if !self.work_map.contains_key(task_id) {
            return Err("Given task is not in the store".into());
        }
        self.work_map.remove(task_id);
        Ok(())
    }

    pub fn get_work_by_id(&mut self, task_id: &T::Key) -> Option<&mut T> {
        match self.work_map.get_mut(task_id) {
            Some(work_item) => Some(work_item.as_mut()),
            None => None,
        }
    }

    pub fn queue_size(&self) -> usize {
        self.work_queue.len()
    }

    pub fn queue_empty(&self) -> bool {
        self.work_queue.is_empty()
    }

    pub fn pop_queue_top(&mut self) -> Option<&mut T> {
        if self.work_queue.is_empty() {
            return None;
        }
        let task_id: T::Key = self.work_queue.pop_front().unwrap();
        self.get_work_by_id(&task_id)
    }

    pub fn get_work_bucket(&self, work_bucket: &T::Key) -> Option<&Vec<T::Key>> {
        self.work_buckets.get(work_bucket)
    }

    pub fn remove_work_bucket(&mut self, work_bucket: &T::Key) -> Result<()> {
        match self.work_buckets.remove(work_bucket) {
            Some(work_bucket_vec) => {
                let mut new_queue: VecDeque<T::Key> = VecDeque::new();
                for task_id in &self.work_queue {
                    if let Some(task) = self.work_map.get_mut(task_id) {
                        if task.get_work_bucket() != *work_bucket {
                            new_queue.push_back(task.get_work_id());
                        }
                    }
                }
                self.work_queue = new_queue;
                for task_id in work_bucket_vec {
                    self.work_map.remove(&task_id);
                }
                Ok(())
            }
            None => Err("Bucket is not in the store".into()),
        }
    }

    pub fn has_task(&self, task_id: &T::Key) -> bool {
        self.work_map.contains_key(task_id)
    }

    pub fn move_task_to_queue(&mut self, task_id: T::Key) -> Result<()> {
        if self.work_map.contains_key(&task_id) {
            self.work_queue.push_back(task_id);
            return Ok(());
        }
        Err("Given task is not in the store".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct TestTask {
        task_creator: String,
        task_id: String,
    }

    impl QueuedWork for TestTask {
        type Key = String;

        fn get_work_bucket(&self) -> String {
            self.task_creator.to_owned()
        }
        fn get_work_id(&self) -> String {
            self.task_id.to_owned()
        }
    }

    fn add_task_to_store(
        store: &mut QueuedWorkStore<TestTask>,
        task_creator: String,
        task_id: String,
    ) -> Result<()> {
        let task = TestTask {
            task_creator: task_creator,
            task_id: task_id,
        };
        return store.add_to_store(Box::new(task));
    }

    #[test]
    fn test_add_task() {
        let mut queued_store: QueuedWorkStore<TestTask> = QueuedWorkStore::new();
        // Successful add
        {
            let result: Result<()> =
                add_task_to_store(&mut queued_store, "John".to_owned(), "task-7".to_owned());
            assert!(result.is_ok());
        }

        // Failed add
        {
            let result: Result<()> =
                add_task_to_store(&mut queued_store, "Bob".to_owned(), "task-7".to_owned());
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_queue_empty() {
        let mut queued_store: QueuedWorkStore<TestTask> = QueuedWorkStore::new();
        // Assert queue starts empty.
        assert!(queued_store.queue_empty());

        // Add item to queue and assert not empty.
        let add_task_result: Result<()> = add_task_to_store(
            &mut queued_store,
            "Task Creator".to_owned(),
            "task-1".to_owned(),
        );
        assert!(add_task_result.is_ok());
        assert!(!queued_store.queue_empty());

        // Pop item off queue and assert empty.
        queued_store.pop_queue_top();
        assert!(queued_store.queue_empty());

        // Move task back to queue and assert not empty.
        let move_task_result: Result<()> = queued_store.move_task_to_queue("task-1".to_owned());
        assert!(move_task_result.is_ok());
        assert!(!queued_store.queue_empty());

        // Add another item to queue and assert not emtpy.
        let add_task_result: Result<()> = add_task_to_store(
            &mut queued_store,
            "Task Creator".to_owned(),
            "task-2".to_owned(),
        );
        assert!(add_task_result.is_ok());
        assert!(!queued_store.queue_empty());

        // Pop the first item and assert not empty.
        queued_store.pop_queue_top();
        assert!(!queued_store.queue_empty());

        // Pop second item and assert emtpy.
        queued_store.pop_queue_top();
        assert!(queued_store.queue_empty());
    }

    #[test]
    fn test_queue_size() {
        let mut queued_store: QueuedWorkStore<TestTask> = QueuedWorkStore::new();
        // Assert queue size starts at 0.
        assert_eq!(queued_store.queue_size(), 0);

        // Add task to queue and assert size is 1.
        let add_task_result: Result<()> = add_task_to_store(
            &mut queued_store,
            "bucket-1".to_owned(),
            "task-1".to_owned(),
        );
        assert!(add_task_result.is_ok());
        assert_eq!(queued_store.queue_size(), 1);

        // Add task to queue and assert size is 2.
        let add_task_result: Result<()> = add_task_to_store(
            &mut queued_store,
            "bucket-1".to_owned(),
            "task-2".to_owned(),
        );
        assert!(add_task_result.is_ok());
        assert_eq!(queued_store.queue_size(), 2);

        // Pop from queue and assert size is 1.
        queued_store.pop_queue_top();
        assert_eq!(queued_store.queue_size(), 1);

        // Remove bucket-1 and assert queue is empty.
        let remove_bucket_result: Result<()> =
            queued_store.remove_work_bucket(&"bucket-1".to_owned());
        assert!(remove_bucket_result.is_ok());
        assert_eq!(queued_store.queue_size(), 0);
    }

    #[test]
    fn test_work_buckets() {
        let mut queued_store: QueuedWorkStore<TestTask> = QueuedWorkStore::new();
        // Assert bucket not found when store is empty.
        assert!(
            queued_store
                .get_work_bucket(&"bucket-1".to_owned())
                .is_none()
        );

        // Add task-1 to bucket-1 and assert success.
        let add_task_result: Result<()> = add_task_to_store(
            &mut queued_store,
            "bucket-1".to_owned(),
            "task-1".to_owned(),
        );
        assert!(add_task_result.is_ok());

        {
            // Check the contents of bucket-1.
            let bucket1: &Vec<String> = queued_store
                .get_work_bucket(&"bucket-1".to_owned())
                .unwrap();
            assert_eq!("task-1", bucket1[0]);
        }

        // Add task-2 to bucket-1 and assert success.
        let add_task_result: Result<()> = add_task_to_store(
            &mut queued_store,
            "bucket-1".to_owned(),
            "task-2".to_owned(),
        );
        assert!(add_task_result.is_ok());

        {
            // Check the contents of bucket-1.
            let bucket1: &Vec<String> = queued_store
                .get_work_bucket(&"bucket-1".to_owned())
                .unwrap();
            assert_eq!("task-1", bucket1[0]);
            assert_eq!("task-2", bucket1[1]);
        }

        // Remove bucket-1 and assert success.
        let remove_bucket_result: Result<()> =
            queued_store.remove_work_bucket(&"bucket-1".to_owned());
        assert!(remove_bucket_result.is_ok());

        // Assert that bucket-1 is no longer in the store.
        assert!(
            queued_store
                .get_work_bucket(&"bucket-1".to_owned())
                .is_none()
        );
    }

    #[test]
    fn test_has_task() {
        let mut queued_store: QueuedWorkStore<TestTask> = QueuedWorkStore::new();
        let add_task_result: Result<()> =
            add_task_to_store(&mut queued_store, "John".to_owned(), "task-7".to_owned());
        assert!(add_task_result.is_ok());
        assert!(queued_store.has_task(&"task-7".to_owned()));
    }

    #[test]
    fn test_remove_task() {
        let mut queued_store: QueuedWorkStore<TestTask> = QueuedWorkStore::new();
        let add_task_result: Result<()> =
            add_task_to_store(&mut queued_store, "John".to_owned(), "task-7".to_owned());
        assert!(add_task_result.is_ok());

        // Remove task when in store and assert success.
        let remove_task_result: Result<()> = queued_store.remove_task(&"task-7".to_owned());
        assert!(remove_task_result.is_ok());

        // Remove task when not in store and assert error.
        let remove_task_result: Result<()> = queued_store.remove_task(&"task-7".to_owned());
        assert!(remove_task_result.is_err());
    }
}