module.exports = {
	root: true,
	parser: '@typescript-eslint/parser',
	parserOptions: {
		project: './tsconfig.json',
		sourceType: 'module',
	},
	ignorePatterns: [
		'dist/**',
		'node_modules/**',
		'gulpfile.js',
	],
	rules: {},
};
