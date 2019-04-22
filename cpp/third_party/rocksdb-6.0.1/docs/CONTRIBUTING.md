This provides guidance on how to contribute various content to `rocksdb.org`.

## Getting started

You should only have to do these one time.

- Rename this file to `CONTRIBUTING.md`.
- Rename `EXAMPLE-README-FOR-RUNNING-DOCS.md` to `README.md` (replacing the existing `README.md` that came with the template).
- Rename `EXAMPLE-LICENSE` to `LICENSE`.
- Review the [template information](./TEMPLATE-INFORMATION.md).
- Review `./_config.yml`.
- Make sure you update `title`, `description`, `tagline` and `gacode` (Google Analytics) in `./_config.yml`.

## Basic Structure

Most content is written in markdown. You name the file `something.md`, then have a header that looks like this:

```
---
docid: getting-started
title: Getting started with ProjectName
layout: docs
permalink: /docs/getting-started.html
---
```

Customize these values for each document, blog post, etc.

> The filename of the `.md` file doesn't actually matter; what is important is the `docid` being unique and the `permalink` correct and unique too).

## Landing page

Modify `index.md` with your new or updated content.

If you want a `GridBlock` as part of your content, you can do so directly with HTML:

```
<div class="gridBlock">
  <div class="blockElement twoByGridBlock alignLeft">
    <div class="blockContent">
      <h3>Your Features</h3>
      <ul>
        <li>The <a href="http://example.org/">Example</a></li>
        <li><a href="http://example.com">Another Example</a></li>
      </ul>
    </div>
  </div>

  <div class="blockElement twoByGridBlock alignLeft">
    <div class="blockContent">
      <h3>More information</h3>
      <p>
         Stuff here
      </p>
    </div>
  </div>
</div>
```

or with a combination of changing `./_data/features.yml` and adding some Liquid to `index.md`, such as:

```
{% include content/gridblocks.html data_source=site.data.features imagealign="bottom"%}
```

## Blog

To modify a blog post, edit the appopriate markdown file in `./_posts/`.

Adding a new blog post is a four-step process.

> Some posts have a `permalink` and `comments` in the blog post YAML header. You will not need these for new blog posts. These are an artifact of migrating the blog from Wordpress to gh-pages.

1. Create your blog post in `./_posts/` in markdown (file extension `.md` or `.markdown`). See current posts in that folder or `./doc-type-examples/2016-04-07-blog-post-example.md` for an example of the YAML format. **If the `./_posts` directory does not exist, create it**.
  - You can add a `<!--truncate-->` tag in the middle of your post such that you show only the excerpt above that tag in the main `/blog` index on your page.
1. If you have not authored a blog post before, modify the `./_data/authors.yml` file with the `author` id you used in your blog post, along with your full name and Facebook ID to get your profile picture.
1. [Run the site locally](./README.md) to test your changes. It will be at `http://127.0.0.1/blog/your-new-blog-post-title.html`
1. Push your changes to GitHub.

## Docs

To modify docs, edit the appropriate markdown file in `./_docs/`.

To add docs to the site....

1. Add your markdown file to the `./_docs/` folder. See `./doc-type-examples/docs-hello-world.md` for an example of the YAML header format. **If the `./_docs/` directory does not exist, create it**.
  - You can use folders in the `./_docs/` directory to organize your content if you want.
1. Update `_data/nav_docs.yml` to add your new document to the navigation bar. Use the `docid` you put in your doc markdown in as the `id` in the `_data/nav_docs.yml` file.
1. [Run the site locally](./README.md) to test your changes. It will be at `http://127.0.0.1/docs/your-new-doc-permalink.html`
1. Push your changes to GitHub.

## Header Bar

To modify the header bar, change `./_data/nav.yml`.

## Top Level Page

To modify a top-level page, edit the appropriate markdown file in `./top-level/`

If you want a top-level page (e.g., http://your-site.com/top-level.html) -- not in `/blog/` or `/docs/`....

1. Create a markdown file in the root `./top-level/`. See `./doc-type-examples/top-level-example.md` for more information.
1. If you want a visible link to that file, update `_data/nav.yml` to add a link to your new top-level document in the header bar.

   > This is not necessary if you just want to have a page that is linked to from another page, but not exposed as direct link to the user.

1. [Run the site locally](./README.md) to test your changes. It will be at `http://127.0.0.1/your-top-level-page-permalink.html`
1. Push your changes to GitHub.

## Other Changes

- CSS: `./css/main.css` or `./_sass/*.scss`.
- Images: `./static/images/[docs | posts]/....`
- Main Blog post HTML: `./_includes/post.html`
- Main Docs HTML: `./_includes/doc.html`
