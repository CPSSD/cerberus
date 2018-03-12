function createCard(parent) {
  var infoBox = $("<div/>")
    .addClass("card")
    .appendTo(parent);

  var container = $("<div/>")
    .addClass("container")
    .appendTo(infoBox);

  return container;
}

function addProperty(name, value, container) {
  $("<a/>")
    .addClass("ui-all")
    .text(name + ": " + value)
    .appendTo(container);
}

function updateWorkersList() {
  var workersBox = $("#workers");

  $.ajax({
    url: "/api/workers",
    dataType: "json",
    success: function(workers) {
      workersBox.empty();

      workers.forEach(function(workerInfo) {
        var container = createCard(workersBox);

        addProperty("Worker ID", workerInfo.worker_id, container);
        addProperty("Status", workerInfo.status, container);
        addProperty("Operation Status", workerInfo.operation_status, container);
        addProperty("Current Task ID", workerInfo.current_task_id, container);
        addProperty("Task Assignments Failed", workerInfo.task_assignments_failed, container);
      });
    }
  });
}

function updateJobsList() {
  var jobsBox = $("#jobs");

  $.ajax({
    url: "/api/jobs",
    dataType: "json",
    success: function(jobs) {
      jobsBox.empty();

      jobs.forEach(function(jobsInfo) {
        var container = createCard(tasksBox);

        addProperty("Job ID", jobsInfo.job_id, container);
        addProperty("Client ID", jobsInfo.client_id, container);
        addProperty("Binary", jobsInfo.binary_path, container);
        addProperty("Input", jobsInfo.input_directory, container);
        addProperty("Output", jobsInfo.output_directory, container);
        addProperty("Status", jobsInfo.status, container);
        var mapTasksText = jobsInfo.map_tasks_completed + "/" + jobsInfo.map_tasks_total;
        addProperty("Map Tasks Completed", mapTasksText, container);
        var reduceTasksText = jobsInfo.reduce_tasks_completed + "/" + jobsInfo.reduce_tasks_total;
        addProperty("Reduce Tasks Completed", reduceTasksText, container);
      });
    }
  });
}

function updateTasksList() {
  var tasksBox = $("#tasks");

  $.ajax({
    url: "/api/tasks",
    dataType: "json",
    success: function(tasks) {
      tasksBox.empty();

      tasks.forEach(function(taskInfo) {
        var container = createCard(tasksBox);

        addProperty("Task ID", taskInfo.task_id, container);
        addProperty("Job ID", taskInfo.job_id, container);
        addProperty("Task Type", taskInfo.task_type, container);
        addProperty("Assigned Worker ID", taskInfo.assigned_worker_id, container);
        addProperty("Status", taskInfo.status, container);
        addProperty("Failure Count", taskInfo.failure_count, container);
      });
    }
  });
}

function updateFunction() {
  updateWorkersList();
  updateJobsList();
  updateTasksList();
}

$(document).ready(function() {
  updateFunction();

  setInterval(updateFunction, 2000);
});
