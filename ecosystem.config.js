const path = require('path');

const root = __dirname;

module.exports = {
  apps: [
    {
      name: 'dev-env-deploy-worker',
      cwd: root,
      script: path.join(root, '.venv', 'bin', 'python'),
      args: [
        '-m',
        'dev_env_lease_manager.worker',
        '--config',
        path.join(root, 'config', 'environments.json'),
        '--poll-interval',
        process.env.DEV_ENV_DEPLOY_WORKER_POLL_INTERVAL || '5',
        '--timeout-seconds',
        process.env.DEV_ENV_DEPLOY_WORKER_TIMEOUT_SECONDS || '1800',
      ],
      autorestart: true,
      max_restarts: 10,
      restart_delay: 5000,
      env: {
        PYTHONUNBUFFERED: '1',
      },
    },
  ],
};
