module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  testMatch: ["**/src/**/*.test.ts"],
  verbose: true,
  forceExit: true, // 👈 if you still get open handle issues
};