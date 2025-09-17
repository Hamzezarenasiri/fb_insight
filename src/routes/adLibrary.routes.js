import express from 'express';
import { authenticate } from '../middlewares/auth.js';
import { validateBody } from '../middlewares/validate.js';
import { runAdLibrarySchema } from '../schemas/runAdLibrary.schema.js';
import { runAdLibraryController } from '../controllers/adLibrary.controller.js';

export const router = express.Router();

router.post('/run-ad-library', authenticate, validateBody(runAdLibrarySchema), runAdLibraryController);


