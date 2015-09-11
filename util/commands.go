/**
 * Utility functions to create commands
 */
package util

func SetCmd(key string, exptime string, size int) string {
	return fmt.Sprintf("set %s 0 %s %d\r\n", key, exptime, size)
}

func GetCommand(key string) string {
	return fmt.Sprintf("get %s\r\n", key)
}

func DeleteCommand(key string) string {
	return fmt.Sprintf("delete %s\r\n", key)
}

func TouchCommand(key string, exptime string) string {
	return fmt.Sprintf("touch %s %s\r\n", key, exptime)
}
