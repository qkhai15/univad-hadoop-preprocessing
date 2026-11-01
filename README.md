# UniVAD Dataset Preprocessing with Hadoop MapReduce

An academic project component that uses **Java and Hadoop MapReduce** to organize image datasets and generate metadata for a UniVAD-style visual anomaly detection workflow.

> **Scope note:** This repository contains the **dataset preprocessing component**, **not** a full UniVAD model implementation or pre-trained model weights. The UniVAD research model and related image encoders are not included in this upload.

## Included files

- `src/DataMapper.java` – parses input image paths, assigns train/test splits and anomaly labels.
- `src/DataReducer.java` – copies images to the output hierarchy and writes metadata.
- `src/SplitAndMetaJob.java` – Hadoop MapReduce job entry point.
- `docs/mapreduce-workflow.png` – conceptual workflow diagram.

## Processing overview

1. Read lines containing image paths from an HDFS input file.
2. Identify class and normal/anomalous status from each path.
3. Place normal samples in train/test according to configurable split; anomaly samples go to test.
4. Organize output image files and metadata for downstream anomaly detection.

## Running / adaptation

Requires a Java Development Kit and Hadoop MapReduce libraries matching your Hadoop installation. Compile the three Java sources with your environment's Hadoop classpath and submit `SplitAndMetaJob` with **two arguments**: input image list and output directory.

**Before running:** inspect the hardcoded destination root (`/output_private_dataset/...`) in `src/DataMapper.java` and `src/DataReducer.java`, as this was written for the original lab environment. Hadoop cluster, HDFS data, and model assets are **not included**.

## Attribution

This is a component from a team academic project. If publishing or using it in a CV, credit the collaborators and explain the code you personally contributed. Make sure the team has approved public release.
