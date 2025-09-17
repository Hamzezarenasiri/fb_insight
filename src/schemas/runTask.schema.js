import { z } from 'zod';

export const runTaskSchema = z.object({
  fbAccessToken: z.string().min(10),
  FBadAccountId: z.string().min(4),
  start_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  end_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  agencyId: z.string().min(10),
  clientId: z.string().min(10),
  userId: z.string().min(10),
  importListName: z.string().min(1),
  uuid: z.string().min(8),
  ad_objective_id: z.string().optional().nullable(),
  ad_objective_field_expr: z.string().optional().nullable(),
  ai: z.string().optional().nullable(),
});


