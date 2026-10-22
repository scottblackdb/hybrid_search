import { createApp, lakebase, server, serving } from '@databricks/appkit';
import { setupSearchRoutes } from './routes/search-routes';

createApp({
  plugins: [lakebase(), server(), serving()],
  onPluginsReady(appkit) {
    setupSearchRoutes(appkit);
  },
}).catch(console.error);
