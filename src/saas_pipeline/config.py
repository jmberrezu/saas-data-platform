from omegaconf import OmegaConf


def load_config(tenant: str = None, env: str = "base"):
    """
    Carga la configuración base y la combina con la configuración específica de un tenant.
    Args:
        tenant (str, opcional): El identificador del tenant (cliente). Por defecto es None.
        env (str, opcional): El entorno de ejecución (ej. 'base', 'dev'). Por defecto es 'base'.
    Returns:
        DictConfig: La configuración resultante utilizando OmegaConf.
    """
    # 1. Carga la configuración principal según el entorno
    base_conf = OmegaConf.load(f"config/{env}.yaml")
    if tenant:
        try:
            # 2. Intenta cargar la configuración específica del cliente
            tenant_conf = OmegaConf.load(f"config/tenants/{tenant}.yaml")
            # 3. Combina ambas (los valores del tenant sobrescriben a los base)
            return OmegaConf.merge(base_conf, tenant_conf)
        except FileNotFoundError:
            print(f"Warning: No se encontró config para el tenant {tenant}")
    # 4. Retorna la base si no hay tenant o si falló la carga del tenant
    return base_conf
