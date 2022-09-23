package hook

type Hook interface {
	Init(params map[string]string) error
	Mock(req interface{}, fullMethod string) (bool, interface{}, error)
	Before(req interface{}, fullMethod string) error
	After(result interface{}, err error, fullMethod string) error
	Release()
}
