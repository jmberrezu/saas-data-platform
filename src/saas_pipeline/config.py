from omegaconf import OmegaConf


def load_config(tenant: str = None, env: str = "dev"):
    """
    Carga la configuración base y la combina con la configuración específica de un tenant.
    Args:
        tenant (str, opcional): El identificador del tenant (cliente). Por defecto es None.
        env (str, opcional): El entorno de ejecución (ej. 'dev', 'qa', 'main'). Por defecto es 'dev'.
    Returns:
        DictConfig: La configuración resultante utilizando OmegaConf.
    """
    # 1. Carga la configuración base
    base_conf = OmegaConf.load("config/base.yaml")

    # 2. Carga y combina la configuración del entorno (dev, qa, main)
    try:
        env_conf = OmegaConf.load(f"config/env/{env}.yaml")
        conf = OmegaConf.merge(base_conf, env_conf)
    except FileNotFoundError:
        print(f"Warning: No se encontró config para el entorno {env}")
        conf = base_conf

    # 3. Carga y combina la configuración del tenant
    if tenant and tenant not in ["global", "base"]:
        try:
            tenant_conf = OmegaConf.load(f"config/tenants/{tenant}.yaml")
            conf = OmegaConf.merge(conf, tenant_conf)
        except FileNotFoundError:
            pass

    return conf
