use serde::Serialize;
use sqlx::PgPool;

use zksync_health_check::{async_trait, CheckHealth, Health, HealthStatus};

use crate::MainConnectionPool;

#[derive(Debug, Serialize)]
struct ConnectionPoolHealthDetails {
    pool_size: u32,
}

impl ConnectionPoolHealthDetails {
    async fn new(pool: &PgPool) -> Self {
        Self {
            pool_size: pool.size(),
        }
    }
}

// HealthCheck used to verify if we can connect to the database.
// This guarantees that the app can use it's main "communication" channel.
// Used in the /health endpoint
#[derive(Clone, Debug)]
pub struct ConnectionPoolHealthCheck {
    connection_pool: MainConnectionPool,
}

impl ConnectionPoolHealthCheck {
    pub fn new(connection_pool: MainConnectionPool) -> ConnectionPoolHealthCheck {
        Self { connection_pool }
    }
}

#[async_trait]
impl CheckHealth for ConnectionPoolHealthCheck {
    fn name(&self) -> &'static str {
        "connection_pool"
    }

    async fn check_health(&self) -> Health {
        // This check is rather feeble, plan to make reliable here:
        // https://linear.app/matterlabs/issue/PLA-255/revamp-db-connection-health-check
        self.connection_pool.access_storage().await.unwrap();

        let mut health = Health::from(HealthStatus::Ready);
        if let MainConnectionPool::Real(pool) = &self.connection_pool {
            let details = ConnectionPoolHealthDetails::new(pool).await;
            health = health.with_details(details);
        }
        health
    }
}
