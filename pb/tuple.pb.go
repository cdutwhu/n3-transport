// Code generated by protoc-gen-go. DO NOT EDIT.
// source: tuple.proto

/*
Package pb is a generated protocol buffer package.

It is generated from these files:
	tuple.proto

It has these top-level messages:
	SPOTuple
*/
package pb

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

// core message type for lowest-level data tuples
type SPOTuple struct {
	Subject   string `protobuf:"bytes,1,opt,name=Subject" json:"Subject,omitempty"`
	Object    string `protobuf:"bytes,2,opt,name=Object" json:"Object,omitempty"`
	Predicate string `protobuf:"bytes,3,opt,name=Predicate" json:"Predicate,omitempty"`
	Version   int64  `protobuf:"varint,4,opt,name=Version" json:"Version,omitempty"`
	Context   string `protobuf:"bytes,5,opt,name=Context" json:"Context,omitempty"`
}

func (m *SPOTuple) Reset()                    { *m = SPOTuple{} }
func (m *SPOTuple) String() string            { return proto.CompactTextString(m) }
func (*SPOTuple) ProtoMessage()               {}
func (*SPOTuple) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *SPOTuple) GetSubject() string {
	if m != nil {
		return m.Subject
	}
	return ""
}

func (m *SPOTuple) GetObject() string {
	if m != nil {
		return m.Object
	}
	return ""
}

func (m *SPOTuple) GetPredicate() string {
	if m != nil {
		return m.Predicate
	}
	return ""
}

func (m *SPOTuple) GetVersion() int64 {
	if m != nil {
		return m.Version
	}
	return 0
}

func (m *SPOTuple) GetContext() string {
	if m != nil {
		return m.Context
	}
	return ""
}

func init() {
	proto.RegisterType((*SPOTuple)(nil), "pb.SPOTuple")
}

func init() { proto.RegisterFile("tuple.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 140 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x2e, 0x29, 0x2d, 0xc8,
	0x49, 0xd5, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x2a, 0x48, 0x52, 0xea, 0x63, 0xe4, 0xe2,
	0x08, 0x0e, 0xf0, 0x0f, 0x01, 0x09, 0x0b, 0x49, 0x70, 0xb1, 0x07, 0x97, 0x26, 0x65, 0xa5, 0x26,
	0x97, 0x48, 0x30, 0x2a, 0x30, 0x6a, 0x70, 0x06, 0xc1, 0xb8, 0x42, 0x62, 0x5c, 0x6c, 0xfe, 0x10,
	0x09, 0x26, 0xb0, 0x04, 0x94, 0x27, 0x24, 0xc3, 0xc5, 0x19, 0x50, 0x94, 0x9a, 0x92, 0x99, 0x9c,
	0x58, 0x92, 0x2a, 0xc1, 0x0c, 0x96, 0x42, 0x08, 0x80, 0xcc, 0x0b, 0x4b, 0x2d, 0x2a, 0xce, 0xcc,
	0xcf, 0x93, 0x60, 0x51, 0x60, 0xd4, 0x60, 0x0e, 0x82, 0x71, 0x41, 0x32, 0xce, 0xf9, 0x79, 0x25,
	0xa9, 0x15, 0x25, 0x12, 0xac, 0x10, 0x9b, 0xa0, 0xdc, 0x24, 0x36, 0xb0, 0xdb, 0x8c, 0x01, 0x01,
	0x00, 0x00, 0xff, 0xff, 0x36, 0xb0, 0xc0, 0xb9, 0xaa, 0x00, 0x00, 0x00,
}