// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: raft.proto

package raftpb

type Entry struct {
	Term        uint64
	Index       uint64
	Type        EntryType
	Key         uint64
	ClientID    uint64
	SeriesID    uint64
	RespondedTo uint64
	Cmd         []byte
}

func (m *Entry) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}