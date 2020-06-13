
class Config:

    prod_config = {
        "sf_oauth": dict(username="", password="",security_token="", domain="login"),
        "platform_host": "",
        "okta_host": "",
        "token_path": "",
        "okta_token": "",
        "forcetalk_host": "",
        "GOOGLE_APPLICATION_CREDENTIALS": "gcp_credentials_prod.json",
        "thought_data_host": "",
        "thought_data_x_api_key": "",
    }

    uat_config = {
        "sf_oauth": dict(username="", password="",security_token="", domain="test"),
        "platform_host": "",
        "okta_host": "",
        "token_path": "",
        "okta_token": "",
        "forcetalk_host": "",
        "GOOGLE_APPLICATION_CREDENTIALS": "gcp_credentials_uat.json",
        "thought_data_host": "",
        "thought_data_x_api_key": "",
    }

    @classmethod
    def get_config(cls, env, key):
        env = Config.get_env_type(env)
        if 'uat' == env:
            return cls.uat_config[key]
        elif 'production' == env:
            return cls.prod_config[key]

    @classmethod
    def get_env_type(cls, env):
        lower_env = str.lower(env)
        if 'uat' in lower_env or 'preprod' in lower_env or 'staging' in lower_env:
            return 'uat'
        elif 'production' in env or 'prod' == lower_env:
            return 'production'
        else:
            raise RuntimeError(f'Invalid env type: {env}')
