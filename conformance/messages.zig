//! The conformance runner's messages, by field number.
//!
//! These come from `connectrpc.conformance.v1` in the suite's `service.proto`
//! and `client_compat.proto`. Only the fields a client under test reads or
//! writes are declared. A field the harness never touches is simply absent; the
//! wire format does not need it.

const std = @import("std");
const pb = @import("protobuf");

/// A header or trailer entry. One name, and the values in wire order.
pub const Header = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .name = pb.string(1),
        .value = pb.string(2),
    };

    name: []const u8 = "",
    value: []const []const u8 = &.{},
};

/// `google.protobuf.Any`: a type name and the serialized message.
pub const Any = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .type_url = pb.string(1),
        .value = pb.bytes(2),
    };

    type_url: []const u8 = "",
    value: []const u8 = "",
};

/// A Connect or gRPC error, as the suite models it.
pub const Error = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .code = pb.field(1),
        .message = pb.string(2),
        .details = pb.field(3),
    };

    /// `connectrpc.conformance.v1.Code`, numerically equal to the gRPC code.
    code: u32 = 0,
    message: ?[]const u8 = null,
    details: []const Any = &.{},
};

/// What the server echoes back. It is the unit the suite compares.
pub const ConformancePayload = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .data = pb.bytes(1),
        .request_info = pb.field(2),
    };

    data: []const u8 = "",
    request_info: ?RequestInfo = null,

    pub const RequestInfo = struct {
        pub const protobuf: pb.Descriptor(@This()) = .{
            .request_headers = pb.field(1),
            .timeout_ms = pb.field(2),
            .requests = pb.field(3),
        };

        request_headers: []const Header = &.{},
        timeout_ms: ?i64 = null,
        requests: []const Any = &.{},
    };
};

/// Every response message of the conformance service carries its payload in
/// field 1, so one holder decodes all four: `UnaryResponse`,
/// `ClientStreamResponse`, `ServerStreamResponse`, and `BidiStreamResponse`.
pub const PayloadHolder = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{ .payload = pb.field(1) };
    payload: ?ConformancePayload = null,
};

/// `google.rpc.Status`, the message inside `grpc-status-details-bin`.
pub const RpcStatus = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .code = pb.field(1),
        .message = pb.string(2),
        .details = pb.field(3),
    };

    code: u32 = 0,
    message: ?[]const u8 = null,
    details: []const Any = &.{},
};

pub const ClientErrorResult = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{ .message = pb.string(1) };
    message: []const u8 = "",
};

pub const ClientResponseResult = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .response_headers = pb.field(1),
        .payloads = pb.field(2),
        .err = pb.field(3),
        .response_trailers = pb.field(4),
        .num_unsent_requests = pb.field(5),
    };

    response_headers: []const Header = &.{},
    payloads: []const ConformancePayload = &.{},
    err: ?Error = null,
    response_trailers: []const Header = &.{},
    num_unsent_requests: i32 = 0,
};

pub const ClientCompatResponse = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .test_name = pb.string(1),
        .response = pb.field(2),
        .err = pb.field(3),
    };

    test_name: []const u8 = "",
    response: ?ClientResponseResult = null,
    err: ?ClientErrorResult = null,
};

pub const TLSCreds = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .cert = pb.bytes(1),
        .key = pb.bytes(2),
    };

    cert: []const u8 = "",
    key: []const u8 = "",
};

/// Timing for a cancellation the suite asks the client to perform.
pub const Cancel = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .before_close_send = pb.field(1),
        .after_close_send_ms = pb.field(2),
        .after_num_responses = pb.field(3),
    };

    /// A oneof on the wire. At most one is set.
    before_close_send: ?Empty = null,
    after_close_send_ms: ?u32 = null,
    after_num_responses: ?u32 = null,

    pub const Empty = struct {
        pub const protobuf: pb.Descriptor(@This()) = .{};
    };
};

pub const ClientCompatRequest = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .test_name = pb.string(1),
        .http_version = pb.field(2),
        .protocol = pb.field(3),
        .codec = pb.field(4),
        .compression = pb.field(5),
        .host = pb.string(6),
        .port = pb.field(7),
        .server_tls_cert = pb.bytes(8),
        .client_tls_creds = pb.field(9),
        .message_receive_limit = pb.field(10),
        .service = pb.string(11),
        .method = pb.string(12),
        .stream_type = pb.field(13),
        .use_get_http_method = pb.field(14),
        .request_headers = pb.field(15),
        .request_messages = pb.field(16),
        .timeout_ms = pb.field(17),
        .request_delay_ms = pb.field(18),
        .cancel = pb.field(19),
    };

    test_name: []const u8 = "",
    http_version: u32 = 0,
    protocol: u32 = 0,
    codec: u32 = 0,
    compression: u32 = 0,
    host: []const u8 = "",
    port: u32 = 0,
    server_tls_cert: []const u8 = "",
    client_tls_creds: ?TLSCreds = null,
    message_receive_limit: u32 = 0,
    service: ?[]const u8 = null,
    method: ?[]const u8 = null,
    stream_type: u32 = 0,
    use_get_http_method: bool = false,
    request_headers: []const Header = &.{},
    request_messages: []const Any = &.{},
    timeout_ms: ?u32 = null,
    request_delay_ms: u32 = 0,
    cancel: ?Cancel = null,
};

/// The enum values this harness reads or writes, from `config.proto`.
pub const protocol_grpc: u32 = 2;
pub const codec_proto: u32 = 1;
pub const compression_identity: u32 = 1;
pub const compression_gzip: u32 = 2;
pub const stream_type_unary: u32 = 1;
pub const stream_type_client_stream: u32 = 2;
pub const stream_type_server_stream: u32 = 3;
pub const stream_type_half_duplex: u32 = 4;
pub const stream_type_full_duplex: u32 = 5;
pub const http_version_2: u32 = 2;
