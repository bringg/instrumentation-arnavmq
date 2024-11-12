import globals from 'globals';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import js from '@eslint/js';
import { FlatCompat } from '@eslint/eslintrc';
import tseslint from 'typescript-eslint';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const compat = new FlatCompat({
  baseDirectory: __dirname,
  recommendedConfig: js.configs.recommended,
  allConfig: js.configs.all,
});

export default [
  {
    ignores: ['**/dist/', '**/node_modules/', 'example', '**/.nyc_output/', '**/.mocharc.js', 'test/env.js'],
  },
  ...compat.extends('eslint-config-prettier'),
  ...tseslint.configs.recommended,
  {
    files: ['**/*.ts', '**/*.js'],
    languageOptions: {
      globals: {
        ...globals.node,
        ...globals.mocha,
      },

      ecmaVersion: 5,
      sourceType: 'commonjs',
      parserOptions: {
        project: ['./tsconfig.json', './test/tsconfig.json'],
      },
    },

    rules: {
      'no-param-reassign': ['error', { props: true }],
      'no-await-in-loop': 0,
      'comma-dangle': 0,
      'max-classes-per-file': 0,

      'max-len': [
        'error',
        {
          code: 190,
          ignoreComments: true,
          ignoreUrls: true,
        },
      ],

      'no-underscore-dangle': [
        1,
        {
          allowAfterThis: true,
        },
      ],

      'no-return-await': 'off',
    },
  },
  {
    files: ['test/**/*.ts', 'test/**/*.js'],
    rules: {
      'prefer-arrow-callback': 'off',
      'func-names': 'off',
      'import/no-extraneous-dependencies': 'off',
      'no-unused-expressions': 'off',
      '@typescript-eslint/no-unused-expressions': 'off',
    },
  },
];
