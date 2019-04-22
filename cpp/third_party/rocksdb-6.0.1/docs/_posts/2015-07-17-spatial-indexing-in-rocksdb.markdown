---
title: Spatial indexing in RocksDB
layout: post
author: icanadi
category: blog
redirect_from:
  - /blog/2039/spatial-indexing-in-rocksdb/
---

About a year ago, there was a need to develop a spatial database at Facebook. We needed to store and index Earth's map data. Before building our own, we looked at the existing spatial databases. They were all very good technology, but also general purpose. We could sacrifice a general-purpose API, so we thought we could build a more performant database, since it would be specifically designed for our use-case. Furthermore, we decided to build the spatial database on top of RocksDB, because we have a lot of operational experience with running and tuning RocksDB at a large scale.

<!--truncate-->

When we started looking at this project, the first thing that surprised us was that our planet is not that big. Earth's entire map data can fit in memory on a reasonably high-end machine. Thus, we also decided to build a spatial database optimized for memory-resident dataset.

The first use-case of our spatial database was an experimental map renderer. As part of our project, we successfully loaded [Open Street Maps](https://www.openstreetmap.org/) dataset and hooked it up with [Mapnik](http://mapnik.org/), a map rendering engine.

The usual Mapnik workflow is to load the map data into a SQL-based database and then define map layers with SQL statements. To render a tile, Mapnik needs to execute a couple of SQL queries. The benefit of this approach is that you don't need to reload your database when you change your map style. You can just change your SQL query and Mapnik picks it up. In our model, we decided to precompute the features we need for each tile. We need to know the map style before we create the database. However, when rendering the map tile, we only fetch the features that we need to render.

We haven't open sourced the RocksDB Mapnik plugin or the database loading pipeline. However, the spatial indexing is available in RocksDB under a name [SpatialDB](https://github.com/facebook/rocksdb/blob/master/include/rocksdb/utilities/spatial_db.h). The API is focused on map rendering use-case, but we hope that it can also be used for other spatial-based applications.

Let's take a tour of the API. When you create a spatial database, you specify the spatial indexes that need to be built. Each spatial index is defined by a bounding box and granularity. For map rendering, we create a spatial index for each zoom levels. Higher zoom levels have more granularity.



    SpatialDB::Create(
      SpatialDBOptions(),
      "/data/map", {
        SpatialIndexOptions("zoom10", BoundingBox(0, 0, 100, 100), 10),
        SpatialIndexOptions("zoom16", BoundingBox(0, 0, 100, 100), 16)
      }
    );




When you insert a feature (building, street, country border) into SpatialDB, you need to specify the list of spatial indexes that will index the feature. In the loading phase we process the map style to determine the list of zoom levels on which we'll render the feature. For example, we will not render the building on zoom level that shows an entire country. Building will only be indexed on higher zoom level's index. Country borders will be indexes on all zoom levels.



    FeatureSet feature;
    feature.Set("type", "building");
    feature.Set("height", 6);
    db->Insert(WriteOptions(), BoundingBox<double>(5, 5, 10, 10),
               well_known_binary_blob, feature, {"zoom16"});




The indexing part is pretty simple. For each feature, we first find a list of index tiles that it intersects. Then, we add a link from the tile's [quad key](https://msdn.microsoft.com/en-us/library/bb259689.aspx) to the feature's primary key. Using quad keys improves data locality, i.e. features closer together geographically will have similar quad keys. Even though we're optimizing for a memory-resident dataset, data locality is still very important due to different caching effects.

After you're done inserting all the features, you can call an API Compact() that will compact the dataset and speed up read queries.



    db->Compact();




SpatialDB's query specifies: 1) bounding box we're interested in, and 2) a zoom level. We find all tiles that intersect with the query's bounding box and return all features in those tiles.




    Cursor* c = db_->Query(ReadOptions(), BoundingBox<double>(1, 1, 7, 7), "zoom16");
    for (c->Valid(); c->Next()) {
        Render(c->blob(), c->feature_set());
    }




Note: `Render()` function is not part of RocksDB. You will need to use one of many open source map renderers, for example check out [Mapnik](http://mapnik.org/).

TL;DR If you need an embedded spatial database, check out RocksDB's SpatialDB. [Let us know](https://www.facebook.com/groups/rocksdb.dev/) how we can make it better.

If you're interested in learning more, check out this [talk](https://www.youtube.com/watch?v=T1jWsDMONM8).
