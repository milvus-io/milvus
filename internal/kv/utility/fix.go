package utility

import "path"

// CheckDirectoryPrefix checks if the directory is empty or not end with /, and returns the prefix of the directory
func CheckDirectoryPrefix(rootPath string, directory string) string {
	if directory == "" {
		panic("directory is empty")
	}
	// if directory[len(directory)-1] != '/' {
	// 	panic("directory should end with /")
	// }
	prefix := path.Join(rootPath, directory) + "/"
	return prefix
}
