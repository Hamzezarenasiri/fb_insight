import axios from 'axios';
import { config } from '../../config/env.js';

export async function chatComplete(model, messages, temperature = 0.2) {
  const data = { model, messages, temperature };
  const res = await axios.post('https://api.openai.com/v1/chat/completions', data, {
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${config.openaiKey}` },
  });
  return res.data;
}


