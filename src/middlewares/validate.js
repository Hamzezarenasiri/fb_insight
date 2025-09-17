export function validateBody(schema) {
  return (req, res, next) => {
    try {
      const parsed = schema.parse(req.body);
      req.validatedBody = parsed;
      next();
    } catch (err) {
      const issues = err?.issues || err?.errors || err;
      res.status(400).json({ error: 'ValidationError', details: issues });
    }
  };
}


