"""Project settings. There is no need to edit this file unless you want to change values
from the Kedro defaults. For further information, including these default values, see
https://docs.kedro.org/en/stable/kedro_project_setup/settings.html."""

# Instantiated project hooks.
from healthcare_pipeline.hooks import SparkHooks  # noqa: E402

# Hooks are executed in a Last-In-First-Out (LIFO) order.
HOOKS = (SparkHooks(),)

# Installed plugins for which to disable hook auto-registration.
# DISABLE_HOOKS_FOR_PLUGINS = ("kedro-viz",)

from pathlib import Path  # noqa: E402

from kedro_viz.integrations.kedro.sqlite_store import SQLiteStore  # noqa: E402

# Class that manages storing KedroSession data.
SESSION_STORE_CLASS = SQLiteStore
# Keyword arguments to pass to the `SESSION_STORE_CLASS` constructor.
SESSION_STORE_ARGS = {"path": str(Path(__file__).parents[2])}

# Directory that holds configuration.
# CONF_SOURCE = "conf"

# Class that manages how configuration is loaded.
from kedro.config import OmegaConfigLoader  # noqa: E402

CONFIG_LOADER_CLASS = OmegaConfigLoader
# Keyword arguments to pass to the `CONFIG_LOADER_CLASS` constructor.
CONFIG_LOADER_ARGS = {
    "base_env": "base",
    "default_run_env": "local",
    "config_patterns": {
        "spark": ["spark*", "spark*/**"],
    }
}

# Class that manages Kedro's library components.
from kedro.framework.context import KedroContext  # noqa: E402

class ProjectContext(KedroContext):
    project_name = "healthcare_pipeline"
    project_version = "0.1"
    package_name = "healthcare_pipeline"

    def _get_pipelines(self):
        from healthcare_pipeline.pipelines.data_processing import pipeline as data_processing_pipeline
        from healthcare_pipeline.pipelines.analysis import pipeline as analysis_pipeline
        return {
            "data_processing": data_processing_pipeline.create_pipeline(),
            "data_analysis": analysis_pipeline.create_pipeline(),
            "__default__": data_processing_pipeline.create_pipeline() + analysis_pipeline.create_pipeline(),
        }

CONTEXT_CLASS = ProjectContext
