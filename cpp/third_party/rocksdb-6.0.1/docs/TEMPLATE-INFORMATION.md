## Template Details

First, go through `_config.yml` and adjust the available settings to your project's standard. When you make changes here, you'll have to kill the `jekyll serve` instance and restart it to see those changes, but that's only the case with the config file.

Next, update some image assets - you'll want to update `favicon.png`, `logo.svg`, and `og_image.png` (used for Like button stories and Shares on Facbeook) in the `static` folder with your own logos.

Next, if you're going to have docs on your site, keep the `_docs` and `docs` folders, if not, you can safely remove them (or you can safely leave them and not include them in your navigation - Jekyll renders all of this before a client views the site anyway, so there's no performance hit from just leaving it there for a future expansion).

Same thing with a blog section, either keep or delete the `_posts` and `blog` folders.

You can customize your homepage in three parts - the first in the homepage header, which is mostly automatically derived from the elements you insert into your config file. However, you can also specify a series of 'promotional' elements in `_data/promo.yml`. You can read that file for more information.

The second place for your homepage is in `index.md` which contains the bulk of the main content below the header. This is all markdown if you want, but you can use HTML and Jekyll's template tags (called Liquid) in there too. Checkout this folder's index.md for an example of one common template tag that we use on our sites called gridblocks.

The third and last place is in the `_data/powered_by.yml` and `_data/powered_by_highlight.yml` files. Both these files combine to create a section on the homepage that is intended to show a list of companies or apps that are using your project. The `powered_by_highlight` file is a list of curated companies/apps that you want to show as a highlight at the top of this section, including their logos in whatever format you want. The `powered_by` file is a more open list that is just text links to the companies/apps and can be updated via Pull Request by the community. If you don't want these sections on your homepage, just empty out both files and leave them blank.

The last thing you'll want to do is setup your top level navigation bar. You can do this by editing `nav.yml` and keeping the existing title/href/category structure used there. Although the nav is responsive and fairly flexible design-wise, no more than 5 or 6 nav items is recommended.
