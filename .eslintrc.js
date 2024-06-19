module.exports = {
  extends: './node_modules/gts/',
  rules: {
    '@typescript-eslint/no-namespace': 'off',
    'n/no-process-exit': 'off',
  },
  settings: {
    node: {
      allowModules: ['@dotenvx/dotenvx'],
    },
  },
};
