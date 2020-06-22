The full documentation on plugin architecture can be found [here](https://docs.google.com/document/d/1EYEKVjS3raOtn6PFusqYiIJnjrYCb63uznZ4EtX_TBI/edit?usp=sharing).  

# Overview
## Repository Structure
Each plugin will have files organized into subdirectories, following the naming conventions in this repository.  
No files will live in the root level of the repository, unless absolutely necessary.  
The `setup` folder will contain any sample dags or sample data for the plugin. This data will be populated via commands in `bin/setup`. 

## Tests
Each plugin will have compatibility tests, that will verify the plugin functions as expected.  
Comptability tests will be marked with `@pytest.mark.compatibility`, as shown in `tests/test_compatibility.py`.

Each plugin's compatibility tests will be run whenever a CircleCI build occurs.  
CircleCI will deploy all plugins to an airflow instance and then runs each of their compatibility tests.  
This verifies that one plugin will not break another when deployed together.  

## Releases
When initally releasing a plugin, the developer will need to:  
- Verify the master branch of the plugin follow's the plugin repository structure and [release format](https://docs.google.com/document/d/1EYEKVjS3raOtn6PFusqYiIJnjrYCb63uznZ4EtX_TBI/edit#bookmark=id.k7ddntielv9h) of other plugins.  
- Create an initial release for the plugin's repository on github, with v1.0.0.  
- Update the [VERSIONS.md](https://github.com/Raybeam/rb_plugin_deploy/blob/master/VERSIONS.md) file to include a link their plugin's releases page.  
  
When developing additional releses the developer will need to follow the proposed versioning architecture, which includes:  
- Branch and merge Pull Requests according to [best practices](https://docs.google.com/document/d/1EYEKVjS3raOtn6PFusqYiIJnjrYCb63uznZ4EtX_TBI/edit#bookmark=id.4u2bcwxpnpr0).  
- Create a new release with the [proper format](https://docs.google.com/document/d/1EYEKVjS3raOtn6PFusqYiIJnjrYCb63uznZ4EtX_TBI/edit#bookmark=id.k7ddntielv9h).  
- Update the [version number](https://docs.google.com/document/d/1EYEKVjS3raOtn6PFusqYiIJnjrYCb63uznZ4EtX_TBI/edit#bookmark=id.mms7hmrnumau).  