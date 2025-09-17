import { z } from 'zod';

export const runAdLibrarySchema = z.object({
  fbAccessToken: z.string().min(10),
  FBadAccountId: z.string().min(4),
  start_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  end_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  search_page_ids: z.string().optional(),
  max_count: z.number().int().min(1).max(500).optional(),
});


