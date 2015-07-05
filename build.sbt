EclipseKeys.withSource := true

// don't load multiple spark contexts at the same time
parallelExecution in Test := false
