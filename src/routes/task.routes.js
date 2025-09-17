import express from 'express';
import { runTaskController } from '../controllers/task.controller.js';
import { validateBody } from '../middlewares/validate.js';
import { runTaskSchema } from '../schemas/runTask.schema.js';

export const router = express.Router();

router.post('/run-task', validateBody(runTaskSchema), runTaskController);


