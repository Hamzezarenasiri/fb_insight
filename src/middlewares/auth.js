export function authenticate(req, res, next) {
  const token = req.headers['authorization'];
  const staticToken = process.env.STATIC_TOKEN;
  if (!token || token !== staticToken) {
    return res.status(401).send({success: false, message: 'Unauthorized'});
  }
  next();
}


