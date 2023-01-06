package cluster

// PeerPicker 集群节点抽象
type PeerPicker interface {
	AddNode(keys ...string)
	PickNode(key string) string
}
