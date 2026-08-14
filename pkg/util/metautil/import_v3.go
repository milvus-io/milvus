// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package metautil

import "path"

const ImportV3RootPath = "import_v3"

func BuildImportV3JobPath(jobID int64) string {
	return path.Join(ImportV3RootPath, JoinIDPath(jobID))
}

func BuildImportV3ReshardPlanPath(jobID, taskID int64) string {
	return path.Join(BuildImportV3JobPath(jobID), "plans", "reshard", JoinIDPath(taskID))
}

func BuildImportV3PlanningPath(jobID, generation int64) string {
	return path.Join(BuildImportV3JobPath(jobID), "plans", "planning", JoinIDPath(generation))
}

func BuildImportV3ImportPlanPath(jobID, taskID int64) string {
	return path.Join(BuildImportV3JobPath(jobID), "plans", "import", JoinIDPath(taskID))
}

func BuildImportV3ReshardOutputPath(jobID, taskID int64) string {
	return path.Join(BuildImportV3JobPath(jobID), "reshard", JoinIDPath(taskID))
}

func BuildImportV3ImportOutputPath(jobID, taskID int64) string {
	return path.Join(BuildImportV3JobPath(jobID), "import", JoinIDPath(taskID))
}
