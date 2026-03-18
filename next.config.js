/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "standalone",
  serverExternalPackages: ["pg", "pg-pool"],
};
module.exports = nextConfig;
