import eslint from "@eslint/js";
import stylistic from "@stylistic/eslint-plugin";
import globals from "globals";

export default [
  {
    languageOptions: {
      globals: globals.node,
    },
  },
  eslint.configs.recommended,
  stylistic.configs.customize({
    arrowParens: true,
    braceStyle: "1tbs",
    quotes: "double",
    semi: true,
  }),
  {
    rules: {
      "@stylistic/function-call-spacing": ["error", "never"],
      "@stylistic/no-multi-spaces": ["error", {
        ignoreEOLComments: true,
        exceptions: {
          Property: true,
          VariableDeclarator: true,
        },
      }],
    },
  },
];
