from lonelypss.config.config import Config


async def setup_config(config: Config) -> None:
    """Convenience function to setup the configuration (similiar idea to aenter)"""
    await config.setup_incoming_auth()
    try:
        await config.setup_outgoing_auth()
        try:
            await config.setup_db()
        except BaseException:
            await config.teardown_outgoing_auth()
            raise
    except BaseException:
        await config.teardown_incoming_auth()
        raise


async def teardown_config(config: Config) -> None:
    """Convenience function to teardown the configuration (similiar idea to aenter)"""
    try:
        await config.teardown_db()
    finally:
        try:
            await config.teardown_outgoing_auth()
        finally:
            await config.teardown_incoming_auth()
