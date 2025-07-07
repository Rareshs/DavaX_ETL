-- project
INSERT INTO project (project_id, contractor, description) VALUES (1, 'myself', 'concediu');
INSERT INTO project (project_id, contractor, description) VALUES (2, 'endava', 'DavaXAcademy');

-- task
INSERT INTO task (task_id, description) VALUES (1, 'concediu');
INSERT INTO task (task_id, description) VALUES (2, 'training');

-- project_task
INSERT INTO project_task (project_id, task_id) VALUES (1, 1);
INSERT INTO project_task (project_id, task_id) VALUES (2, 2);



