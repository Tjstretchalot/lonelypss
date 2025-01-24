from lonelypss.config.config import Config


async def setup_config(config: Config) -> None:
    """Convenience function to setup the configuration (similiar idea to aenter)"""
    await config.setup_to_broadcaster_auth()
    try:
        await config.setup_to_subscriber_auth()
        try:
            await config.setup_db()
            try:
                await config.setup_http_notify_client_session(config)
                try:
                    await config.setup_tracing()
                except BaseException:
                    await config.teardown_http_notify_client_session()
                    raise
            except BaseException:
                await config.teardown_db()
                raise
        except BaseException:
            await config.teardown_to_subscriber_auth()
            raise
    except BaseException:
        await config.teardown_to_broadcaster_auth()
        raise


async def teardown_config(config: Config) -> None:
    """Convenience function to teardown the configuration (similiar idea to aenter)"""
    try:
        await config.teardown_tracing()
    finally:
        try:
            await config.teardown_http_notify_client_session()
        finally:
            try:
                await config.teardown_db()
            finally:
                try:
                    await config.teardown_to_subscriber_auth()
                finally:
                    await config.teardown_to_broadcaster_auth()
