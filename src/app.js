import express from 'express';
import { router as taskRouter } from './routes/task.routes.js';
import { router as adLibraryRouter } from './routes/adLibrary.routes.js';
import { errorHandler } from './middlewares/error.js';
import swaggerUi from 'swagger-ui-express';
import YAML from 'yamljs';
import path from 'path';
import { fileURLToPath } from 'url';

export const app = express();
app.set('trust proxy', 1);
app.use(express.json({ limit: '10mb' }));

app.use(taskRouter);
app.use(adLibraryRouter);

// OpenAPI /docs
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const openapiPath = path.join(__dirname, 'docs', 'openapi.yaml');
const openapiSpec = YAML.load(openapiPath);
app.use('/docs', swaggerUi.serve, swaggerUi.setup(openapiSpec));

app.use(errorHandler);


