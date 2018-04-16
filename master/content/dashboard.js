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


function addProperty(name, value, id, container) {
  var row = $("<tr/>").appendTo(container);
  $("<td>").text(name).appendTo(row);
  $("<td>").text(value).attr("id", id).appendTo(row);
}

function addButton(text, clickedFuncCreator, id, container) {
  var button = $("<button/>")
    .addClass("button")
    .text(text)
    .attr("id", id);

  var clickedFunc = clickedFuncCreator(button);

  button
    .click(clickedFunc)
    .appendTo(container);
}

function createCardWithProperties(parent, infoTable, propertiesInfo, halfSize) {
  var container = createCard(parent, halfSize);

  for (var i in propertiesInfo) {
    var propInfo = propertiesInfo[i];
    addProperty(propInfo.name, infoTable[propInfo.key], propInfo.key, container);
  }

  return container;
}

function updateCard(container, infoTable, propertiesInfo, halfSize) {
  if (halfSize) {
    container.parent().parent().css({
      "width": "22.5%",
    });
  } else {
    container.parent().parent().css({
      "width": "45%",
    });
  }

  for (var i in propertiesInfo) {
    var propInfo = propertiesInfo[i];
    var elem = container.find("#" + propInfo.key);
    elem.text(infoTable[propInfo.key]);
  }
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

var workerProperties = [{
    name: "Worker ID",
    key: "worker_id",
  },
  {
    name: "Address",
    key: "address",
  },
  {
    name: "Status",
    key: "status",
  },
  {
    name: "Operation Status",
    key: "operation_status",
  },
  {
    name: "Current Task ID",
    key: "current_task_id",
  },
  {
    name: "Task Assignments Failed",
    key: "task_assignments_failed",
  },
];

// Map of workerId to card
var workerCardsMap = {};

function updateWorkersList() {
  var workersBox = $("#workers");

  $.ajax({
    url: "/api/workers",
    dataType: "json",
    success: function(workers) {
      var aliveWorkers = {};

      workers.forEach(function(workerInfo) {
        aliveWorkers[workerInfo.worker_id] = true;

        if (workerCardsMap[workerInfo.worker_id]) {
          var container = workerCardsMap[workerInfo.worker_id];

          updateCard(
            container,
            workerInfo,
            workerProperties,
            /* halfSize = */
            (workers.length > 6)
          );
        } else {
          var container = createCardWithProperties(
            workersBox,
            workerInfo,
            workerProperties,
            /* halfSize = */
            (workers.length > 6)
          );

          addButton(
            "View Logs",
            showLogsFunction(workerInfo.worker_id),
            "view_logs",
            container.parent()
          );

          workerCardsMap[workerInfo.worker_id] = container;
        }
      });

      for (var workerId in workerCardsMap) {
        var card = workerCardsMap[workerId];
        if (!aliveWorkers[workerId]) {
          card.parent().parent().remove();
          delete workerCardsMap[workerId];
        }
      }
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
        },
        error: function(xhr, status, error) {
          console.log("Error canceling job:");
          console.log(error);
          button.attr("disabled", false);
          button.text("Cancel Job");
          button.css({
            "background-color": "#008CBA",
          });
        }
      });
    }
  }
}

var jobProperties = [{
    name: "Job ID",
    key: "job_id",
  },
  {
    name: "Client ID",
    key: "client_id",
  },
  {
    name: "Priority",
    key: "priority",
  },
  {
    name: "Binary",
    key: "binary_path",
  },
  {
    name: "Input",
    key: "input_directory",
  },
  {
    name: "Output",
    key: "output_directory",
  },
  {
    name: "Status",
    key: "status",
  },
  {
    name: "Map Tasks Completed",
    key: "map_tasks_text",
  },
  {
    name: "Reduce Tasks Completed",
    key: "reduce_tasks_text",
  },
];

// Map of jobId to card
var jobCardsMap = {};

function updateJobsList() {
  var jobsBox = $("#jobs");

  $.ajax({
    url: "/api/jobs",
    dataType: "json",
    success: function(jobs) {
      var jobsAlive = {};

      jobs.forEach(function(jobsInfo) {
        jobsAlive[jobsInfo.job_id] = true;

        jobsInfo.map_tasks_text = jobsInfo.map_tasks_completed + "/" +
          jobsInfo.map_tasks_total;
        jobsInfo.reduce_tasks_text = jobsInfo.reduce_tasks_completed + "/" +
          jobsInfo.reduce_tasks_total;

        if (jobCardsMap[jobsInfo.job_id]) {
          var container = jobCardsMap[jobsInfo.job_id];

          updateCard(
            container,
            jobsInfo,
            jobProperties
          );

          if (jobsInfo.status == "IN_PROGRESS" || jobsInfo.status == "IN_QUEUE") {
            if (container.parent().find("#cancel_button").length == 0) {
              addButton("Cancel Job", cancelJobFunction(jobsInfo.job_id), "cancel_button", container.parent());
            }
          } else {
            container.parent().find("#cancel_button").remove();
          }
        } else {
          var container = createCardWithProperties(jobsBox, jobsInfo, jobProperties);

          if (jobsInfo.status == "IN_PROGRESS" || jobsInfo.status == "IN_QUEUE") {
            addButton("Cancel Job",
              cancelJobFunction(jobsInfo.job_id),
              "cancel_button",
              container.parent()
            );
          }

          jobCardsMap[jobsInfo.job_id] = container;
        }
      });

      for (var jobId in jobCardsMap) {
        var card = jobCardsMap[jobId];
        if (!jobsAlive[jobId]) {
          card.parent().parent().remove();
          delete jobCardsMap[jobId];
        }
      }
    }
  });
}

var taskProperties = [{
    name: "Task ID",
    key: "task_id",
  },
  {
    name: "Job ID",
    key: "job_id",
  },
  {
    name: "Task Type",
    key: "task_type",
  },
  {
    name: "Assigned Worker ID",
    key: "assigned_worker_id",
  },
  {
    name: "Status",
    key: "status",
  },
  {
    name: "Output",
    key: "output_directory",
  },
  {
    name: "Status",
    key: "status",
  },
  {
    name: "Failure Count",
    key: "failure_count",
  },
];

// Map of taskId to card
var taskCardsMap = {};

function updateTasksList() {
  var tasksBox = $("#tasks");

  $.ajax({
    url: "/api/tasks",
    dataType: "json",
    success: function(tasks) {
      var tasksAlive = {};

      tasks.forEach(function(taskInfo) {
        tasksAlive[taskInfo.task_id] = true;

        if (taskCardsMap[taskInfo.task_id]) {
          var container = taskCardsMap[taskInfo.task_id];

          updateCard(
            container,
            taskInfo,
            taskProperties
          );
        } else {
          var container = createCardWithProperties(tasksBox, taskInfo, taskProperties);

          taskCardsMap[taskInfo.task_id] = container;
        }
      });

      for (var taskId in taskCardsMap) {
        var card = taskCardsMap[taskId];
        if (!tasksAlive[taskId]) {
          card.parent().parent().remove();
          delete taskCardsMap[taskId];
        }
      }
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
