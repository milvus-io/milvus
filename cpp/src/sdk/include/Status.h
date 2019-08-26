#pragma once

#include <string>
#include <sstream>

/** \brief Milvus SDK namespace
*/
namespace milvus {

/**
* @brief Status Code for SDK interface return
*/
enum class StatusCode {
    OK = 0,
// system error section
    UnknownError = 1,
    NotSupported,
    NotConnected,

// function error section
    InvalidAgument = 1000,
    RPCFailed,
    ServerFailed,
};

/**
* @brief Status for SDK interface return
*/
class Status {
public:
    /**
     * @brief Status
     *
     * Default constructor.
     *
     */
    Status() = default;

    /**
     * @brief Status
     *
     * Destructor.
     *
     */
    ~Status() noexcept;

    /**
     * @brief Status
     *
     * Constructor
     *
     * @param code, status code.
     * @param message, status message.
     *
     */
    Status(StatusCode code, const std::string &message);

    /**
     * @brief Status
     *
     * Copy constructor
     *
     * @param status, status to be copied.
     *
     */
    inline
    Status(const Status &status);

    /**
     * @brief Status
     *
     * Assignment operator
     *
     * @param status, status to be copied.
     * @return, the status is assigned.
     *
     */
    Status
    &operator=(const Status &s);

    /**
     * @brief Status
     *
     * Move constructor
     *
     * @param status, status to be moved.
     *
     */
    Status(Status &&s) noexcept ;

    /**
     * @brief Status
     *
     * Move assignment operator
     *
     * @param status, status to be moved.
     * @return, the status is moved.
     *
     */
    Status
    &operator=(Status &&s) noexcept;

    /**
     * @brief Status
     *
     * AND operator
     *
     * @param status, status to be AND.
     * @return, the status after AND operation.
     *
     */
    inline
    Status operator&(const Status &s) const noexcept;

    /**
     * @brief Status
     *
     * AND operator
     *
     * @param status, status to be AND.
     * @return, the status after AND operation.
     *
     */
    inline
    Status operator&(Status &&s) const noexcept;

    /**
     * @brief Status
     *
     * AND operator
     *
     * @param status, status to be AND.
     * @return, the status after AND operation.
     *
     */
    inline
    Status &operator&=(const Status &s) noexcept;

    /**
     * @brief Status
     *
     * AND operator
     *
     * @param status, status to be AND.
     * @return, the status after AND operation.
     *
     */
    inline
    Status &operator&=(Status &&s) noexcept;

    /**
     * @brief OK
     *
     * static OK status constructor
     *
     * @return, the status with OK.
     *
     */
    static
    Status OK() { return Status(); }

    /**
     * @brief OK
     *
     * static OK status constructor with a specific message
     *
     * @param, serveral specific messages
     * @return, the status with OK.
     *
     */
    template<typename... Args>
    static Status
    OK(Args &&... args) {
        return Status(StatusCode::OK, MessageBuilder(std::forward<Args>(args)...));
    }

/**
 * @brief Invalid
 *
 * static Invalid status constructor with a specific message
 *
 * @param, serveral specific messages
 * @return, the status with Invalid.
 *
 */
template<typename... Args>
static Status
Invalid(Args &&... args) {
    return Status(StatusCode::InvalidAgument,
                  MessageBuilder(std::forward<Args>(args)...));
}

/**
 * @brief Unknown Error
 *
 * static unknown error status constructor with a specific message
 *
 * @param, serveral specific messages
 * @return, the status with unknown error.
 *
 */
template<typename... Args>
static Status
UnknownError(Args &&... args) {
    return Status(StatusCode::UnknownError, MessageBuilder(std::forward<Args>(args)...));
}

/**
 * @brief not supported Error
 *
 * static not supported status constructor with a specific message
 *
 * @param, serveral specific messages
 * @return, the status with not supported error.
 *
 */
template<typename... Args>
static Status
NotSupported(Args &&... args) {
    return Status(StatusCode::NotSupported, MessageBuilder(std::forward<Args>(args)...));
}

/**
 * @brief ok
 *
 * Return true iff the status indicates success.
 *
 * @return, if the status indicates success.
 *
 */
bool
ok() const { return (state_ == nullptr); }

/**
 * @brief IsInvalid
 *
 * Return true iff the status indicates invalid.
 *
 * @return, if the status indicates invalid.
 *
 */
bool
IsInvalid() const { return code() == StatusCode::InvalidAgument; }

/**
 * @brief IsUnknownError
 *
 * Return true iff the status indicates unknown error.
 *
 * @return, if the status indicates unknown error.
 *
 */
bool
IsUnknownError() const { return code() == StatusCode::UnknownError; }

/**
 * @brief IsNotSupported
 *
 * Return true iff the status indicates not supported.
 *
 * @return, if the status indicates not supported.
 *
 */
bool
IsNotSupported() const { return code() == StatusCode::NotSupported; }

/**
 * @brief ToString
 *
 * Return error message string.
 *
 * @return, error message string.
 *
 */
std::string
ToString() const;

/**
 * @brief CodeAsString
 *
 * Return a string representation of the status code.
 *
 * @return, a string representation of the status code.
 *
 */
std::string
CodeAsString() const;

/**
 * @brief code
 *
 * Return the StatusCode value attached to this status.
 *
 * @return, the status code value attached to this status.
 *
 */
StatusCode
code() const { return ok() ? StatusCode::OK : state_->code; }

/**
 * @brief message
 *
 * Return the specific error message attached to this status.
 *
 * @return, the specific error message attached to this status.
 *
 */
std::string
message() const { return ok() ? "" : state_->message; }

private:
struct State {
    StatusCode code;
    std::string message;
};

// OK status has a `nullptr` state_.  Otherwise, `state_` points to
// a `State` structure containing the error code and message.
State *state_ = nullptr;

void
DeleteState() {
    delete state_;
    state_ = nullptr;
}

void
CopyFrom(const Status &s);

inline void
MoveFrom(Status &s);

template<typename Head>
static void
MessageBuilderRecursive(std::stringstream &stream, Head &&head) {
    stream << head;
}

template<typename Head, typename... Tail>
static void
MessageBuilderRecursive(std::stringstream &stream, Head &&head, Tail &&... tail) {
    MessageBuilderRecursive(stream, std::forward<Head>(head));
    MessageBuilderRecursive(stream, std::forward<Tail>(tail)...);
}

template<typename... Args>
static std::string
MessageBuilder(Args &&... args) {
    std::stringstream stream;

    MessageBuilderRecursive(stream, std::forward<Args>(args)...);

    return stream.str();
}
};
}