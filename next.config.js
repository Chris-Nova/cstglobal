/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "standalone",
  serverExternalPackages: ["pg", "pg-pool"],
  typescript: {
    ignoreBuildErrors: true,
  },
};

module.exports = nextConfig;
