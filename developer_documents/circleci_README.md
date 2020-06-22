Once a new plugin repository is created, you can sign into CircleCI to set up [Continuous Integration](https://app.circleci.com/projects).  
  
After signing in to Raybeam's organization, you should see **Setup Project** next to your repository's name.  
  
Assuming you copied the `.circleci/config.yml` from this repository into your plugin's repository, the next steps are:  
- click **Start Building**  
- click **Add Manually**  
- click **Start Building**  
- click **Add Projects** on the left of the page  
- click on your repository's name  
- click **Project Settings** in the top right  
- click **SSH Keys** in the menu on the left  
- click **Add User Key**

  
On your next Pull Request, you should see CircleCI build and test your project.