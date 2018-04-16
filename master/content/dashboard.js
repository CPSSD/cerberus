function createCard(parent, halfSize) {
  var infoBox = $("<div/>")
    .addClass("card");

  if (halfSize) {
    infoBox.css({
      "width": "22.5%",
    });
  }

  var container = $("<div/>")
    .addClass("container")
    .appendTo(infoBox);

  var table = $("<table/>")
    .addClass("stats-table")
    .appendTo(container);

  infoBox.appendTo(parent);

  return table;
}

function addProperty(name, value, container) {
  var row = $("<tr/>").appendTo(container);
  $("<td>").text(name).appendTo(row);
  $("<td>").text(value).appendTo(row);
}

function addButton(text, clickedFuncCreator, container) {
  var button = $("<button/>")
    .addClass("button")
    .text(text);

  var clickedFunc = clickedFuncCreator(button);

  button
    .click(clickedFunc)
    .appendTo(container);
}

function showWorkerLogs(workerId, logsText) {
  var workerLogsText = $("#worker-logs-text");
  workerLogsText.text(logsText);

  var workerScroll = $("#worker-scroll-box");

  var logsBox = $("#worker-logs-box");
  logsBox.text("Worker ID: " + workerId);

  workerScroll.appendTo(logsBox);

  var logsView = $("#worker-logs");
  logsView.css({
    "visibility": "visible",
  });
}

function closeWorkerLogs() {
  var logsView = $("#worker-logs");
  logsView.css({
    "visibility": "hidden",
  })
}

function showLogsFunction(workerId) {
  return function(button) {
    return function() {
      button.attr("disabled", true);
      button.text("Requesting logs");
      button.css({
        "background-color": "#B3E5FC",
      });

      $.ajax({
        url: "/api/workerlogs/query?worker_id=" + encodeURIComponent(workerId),
        dataType: "text",
        success: function(logsText) {
          showWorkerLogs(workerId, logsText);
          button.attr("disabled", false);
          button.text("View Logs");
          button.css({
            "background-color": "#008CBA",
          });
        },
        error: function(xhr, status, error) {
          console.log("Error getting worker logs:");
          console.log(error);
          button.attr("disabled", false);
          button.text("View Logs");
          button.css({
            "background-color": "#008CBA",
          });
        }
      });
    }
  }
}

function updateWorkersList() {
  var workersBox = $("#workers");

  $.ajax({
    url: "/api/workers",
    dataType: "json",
    success: function(workers) {
      workersBox.empty();

      workers.forEach(function(workerInfo) {
        var container = createCard(workersBox, /* halfSize = */ (workers.length > 6));

        addProperty("Worker ID", workerInfo.worker_id, container);
        addProperty("Address", workerInfo.address, container);
        addProperty("Status", workerInfo.status, container);
        addProperty("Operation Status", workerInfo.operation_status, container);
        addProperty("Current Task ID", workerInfo.current_task_id, container);
        addProperty("Task Assignments Failed", workerInfo.task_assignments_failed, container);
        addButton("View Logs", showLogsFunction(workerInfo.worker_id), container.parent());
      });
    }
  });
}

function cancelJobFunction(jobId) {
  return function(button) {
    return function() {
      button.attr("disabled", true);
      button.text("Canceling job");
      button.css({
        "background-color": "#B3E5FC",
      });

      $.ajax({
        url: "/api/canceljob/query?job_id=" + encodeURIComponent(jobId),
        dataType: "json",
        success: function() {
          updateJobsList();
        }
      });
    }
  }
}

function updateJobsList() {
  var jobsBox = $("#jobs");

  $.ajax({
    url: "/api/jobs",
    dataType: "json",
    success: function(jobs) {
      jobsBox.empty();

      jobs.forEach(function(jobsInfo) {
        var container = createCard(jobsBox);

        addProperty("Job ID", jobsInfo.job_id, container);
        addProperty("Client ID", jobsInfo.client_id, container);
        addProperty("Priority", jobsInfo.priority, container);
        addProperty("Binary", jobsInfo.binary_path, container);
        addProperty("Input", jobsInfo.input_directory, container);
        addProperty("Output", jobsInfo.output_directory, container);
        addProperty("Status", jobsInfo.status, container);
        var mapTasksText = jobsInfo.map_tasks_completed + "/" + jobsInfo.map_tasks_total;
        addProperty("Map Tasks Completed", mapTasksText, container);
        var reduceTasksText = jobsInfo.reduce_tasks_completed + "/" + jobsInfo.reduce_tasks_total;
        addProperty("Reduce Tasks Completed", reduceTasksText, container);
        if (jobsInfo.status == "IN_PROGRESS" || jobsInfo.status == "IN_QUEUE") {
          addButton("Cancel Job", cancelJobFunction(jobsInfo.job_id), container.parent());
        }
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

function updateMasterLog() {
  var logs = $("#master-logs");
  $.ajax({
    url: "/api/logs",
    dataType: "text",
    success: function(logsText) {
      logs.text(logsText);
    }
  });
}

function updateFunction() {
  updateWorkersList();
  updateJobsList();
  updateTasksList();
  updateMasterLog();
}

var scheduleFormToggled = false;

function toggleScheduleForm() {
  scheduleFormToggled = !scheduleFormToggled;
  var scheduleForm = document.getElementById("schedule-form");
  scheduleForm.style.visibility = scheduleFormToggled ? "visible" : "hidden";
}

function processScheduleMapReduceForm(e) {
  if (e.preventDefault) {
    e.preventDefault();
  }

  var binaryPath = encodeURIComponent($("#binary").val());
  var inputPath = encodeURIComponent($("#input").val());
  var outputPath = encodeURIComponent($("#output").val());
  var priority = encodeURIComponent($("#priority").val());
  var map_size = encodeURIComponent($("#map_size").val());

  var submitButton = $("#submit-job");
  submitButton.attr("disabled", true);
  submitButton.val("Submiting request");
  submitButton.css({
    "background-color": "#B3E5FC",
  });

  var restoreAnimation = function() {
    submitButton.animate({
      "background-color": "#008CBA",
    }, {
      queue: false,
      duration: 1000,
      complete: function() {
        submitButton.attr("disabled", false);
        submitButton.val("Schedule MapReduce");
        submitButton.css({
          "background-color": "#008CBA",
        });
      }
    });
  }

  var requestUrl = "/api/schedule/query?" +
    "binary_path=" + binaryPath +
    "&input_path=" + inputPath +
    "&output_path=" + outputPath +
    "&priority=" + priority +
    "&map_size=" + map_size;


  $.ajax({
    url: requestUrl,
    dataType: "json",
    complete: function() {
      submitButton.val("Succesfully scheduled");
      if (scheduleFormToggled) {
        toggleScheduleForm();
      }
      restoreAnimation();
      updateFunction();
    }
  });

  return false;
}

$(document).ready(function() {
  var scheduleMapReduceForm = document.getElementById("schedule-job");
  if (scheduleMapReduceForm.attachEvent) {
    scheduleMapReduceForm.attachEvent("submit", processScheduleMapReduceForm);
  } else {
    scheduleMapReduceForm.addEventListener("submit", processScheduleMapReduceForm);
  }

  updateFunction();

  setInterval(updateFunction, 2000);
});
