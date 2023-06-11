from cpapi import session, oauth_utils
import json
import os
import time
import logging


def init_api_session(attempt_count: int = 1) -> session.APISession:
    sleep_between_attempts = 2**attempt_count
    logging.info(f"Attempt {attempt_count} to initialise brokerage session")
    oauth_config = json.load(
        open(os.getenv("CPAPI_OAUTH_CONFIG"), "r"),
        object_hook=oauth_utils.oauth_config_hook,
    )
    # logging.info(f"Using OAuth config: {oauth_config}")
    try:
        # oauth_session = session.OAuthSession(oauth_config)
        oauth_session = session.GatewaySession(port=int(os.getenv("CPAPI_GATEWAY_PORT")))
        # response = oauth_session.init_brokerage_session()
        # logging.info(f"Brokerage session response: {response}")
        oauth_session.brokerage_accounts()
        return oauth_session
    except Exception as e:
        logging.error(f"Error initialising brokerage session: {e}")
        logging.info(f"Retrying in {sleep_between_attempts} seconds")
        time.sleep(sleep_between_attempts)
        return init_api_session(attempt_count + 1)


def keep_api_session_alive(api_session: session.APISession):
    """
    Manage the authentication status of the brokerage session.
    """
    AUTH_TIMEOUT = 60
    while True:
        try:
            tickle_response = api_session.tickle()
            logging.info(f"Brokerage session tickle response: {tickle_response}")
            auth_status = api_session.auth_status()
            logging.info(f"Brokerage session status: {auth_status}")
            is_authenticated = auth_status["authenticated"]
            if not is_authenticated:
                # api_session.init_brokerage_session()
                api_session.reauthenticate()
        except Exception as e:
            logging.error(f"Error managing brokerage session: {e}")
        except KeyboardInterrupt:
            logging.info("Exiting session management thread")
            api_session.logout()
            break
        finally:
            time.sleep(AUTH_TIMEOUT)
