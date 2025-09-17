import { parseExpressionAt } from 'acorn';

const ALLOWED_BINARY_OPS = new Set(["+", "-", "*", "/", "**"]);
const ALLOWED_UNARY_OPS = new Set(["+", "-"]);
const ALLOWED_FUNCTIONS = { sqr: (x) => x * x };

function validateNode(node) {
  switch (node.type) {
    case 'Literal':
      if (typeof node.value !== 'number') throw new Error(`Non-numeric literal: ${node.value}`);
      break;
    case 'Identifier':
      break;
    case 'BinaryExpression':
      if (!ALLOWED_BINARY_OPS.has(node.operator)) throw new Error(`Unsupported operator: ${node.operator}`);
      validateNode(node.left); validateNode(node.right);
      break;
    case 'UnaryExpression':
      if (!ALLOWED_UNARY_OPS.has(node.operator)) throw new Error(`Unsupported unary operator: ${node.operator}`);
      validateNode(node.argument);
      break;
    case 'CallExpression':
      if (node.callee.type !== 'Identifier' || !(node.callee.name in ALLOWED_FUNCTIONS) || node.arguments.length !== 1) {
        throw new Error(`Unsupported function call: ${node.callee.name}`);
      }
      validateNode(node.arguments[0]);
      break;
    case 'ExpressionStatement':
      validateNode(node.expression);
      break;
    default:
      throw new Error(`Unsupported syntax node: ${node.type}`);
  }
}

function evaluateNode(node, row) {
  switch (node.type) {
    case 'Literal': return node.value;
    case 'Identifier': return row[node.name];
    case 'BinaryExpression': {
      const l = evaluateNode(node.left, row); const r = evaluateNode(node.right, row);
      if (l == null || r == null) return null;
      switch (node.operator) {
        case '+': return l + r;
        case '-': return l - r;
        case '*': return l * r;
        case '/': return r === 0 ? null : l / r;
        case '**': return Math.pow(l, r);
      }
    }
    case 'UnaryExpression': {
      const v = evaluateNode(node.argument, row);
      if (v == null) return null;
      return node.operator === '-' ? -v : +v;
    }
    case 'CallExpression': {
      const fn = ALLOWED_FUNCTIONS[node.callee.name];
      const arg = evaluateNode(node.arguments[0], row);
      if (arg == null) return null;
      return fn(arg);
    }
    default: return null;
  }
}

function compileFormula(expr) {
  const node = parseExpressionAt(expr, 0, { ecmaVersion: 2020 });
  validateNode(node);
  return (row) => { try { return evaluateNode(node, row); } catch { return null; } };
}

export function buildForwardCalculators(schema) {
  const forward = {};
  for (const { key, formula } of schema) {
    if (!formula || formula.toUpperCase() === 'N/A') forward[key] = null;
    else {
      try { forward[key] = compileFormula(formula); }
      catch (err) { console.warn(`Invalid formula for ${key}: ${err.message}`); forward[key] = null; }
    }
  }
  return forward;
}

export function fillMissingFields(rows, schema, maxIterations = 5) {
  const forward = buildForwardCalculators(schema);
  let iteration = 0;
  while (iteration < maxIterations) {
    let changed = false;
    for (const row of rows) {
      for (const { key } of schema) {
        if (row[key] == null && typeof forward[key] === 'function') {
          const val = forward[key](row);
          if (val != null) { row[key] = val; changed = true; }
        }
      }
    }
    if (!changed) break;
    iteration++;
  }
  return rows;
}

export function parseFormulaOld(formula) {
  const dependentFields = formula.match(/([a-zA-Z_]+)/g) || [];
  const formulaFunction = new Function(...dependentFields, `return ${formula};`);
  return { dependentFields, formulaFunction };
}

export function calculateMetrics(inputValues, metrics) {
  let calculatedValues = { ...inputValues };
  const dependencies = {};
  metrics.forEach(metric => {
    if (metric.formula !== 'N/A') dependencies[metric.key] = parseFormulaOld(metric.formula);
  });
  let pending = true; let previousPendingCount = Object.keys(calculatedValues).length;
  while (pending) {
    pending = false;
    metrics.forEach(metric => {
      if (calculatedValues[metric.key] === undefined && dependencies[metric.key]) {
        const { dependentFields, formulaFunction } = dependencies[metric.key];
        const missing = dependentFields.filter(field => calculatedValues[field] === undefined);
        if (missing.length === 0) {
          const result = formulaFunction(...dependentFields.map(field => calculatedValues[field]));
          if (result !== null && !isNaN(result)) calculatedValues[metric.key] = result;
          pending = true;
        }
      }
    });
    const current = Object.keys(calculatedValues).length;
    if (current === previousPendingCount) break;
    previousPendingCount = current;
  }
  Object.keys(calculatedValues).forEach(key => {
    if (calculatedValues[key] === null || isNaN(calculatedValues[key])) {
      if (!Object.prototype.hasOwnProperty.call(inputValues, key)) delete calculatedValues[key];
    }
  });
  return calculatedValues;
}


