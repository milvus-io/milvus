#ifndef UNDERLYING_FUNCTIONALITIES_HPP
#define UNDERLYING_FUNCTIONALITIES_HPP

#include "crtp.hpp"
#include "named_type_impl.hpp"

#include <functional>
#include <iostream>
#include <memory>

// C++17 constexpr additions
#if FLUENT_CPP17_PRESENT
#    define FLUENT_CONSTEXPR17 constexpr
#else
#    define FLUENT_CONSTEXPR17 
#endif

namespace fluent
{

template <typename T>
struct PreIncrementable : crtp<T, PreIncrementable>
{
    IGNORE_SHOULD_RETURN_REFERENCE_TO_THIS_BEGIN

    FLUENT_CONSTEXPR17 T& operator++()
    {
        ++this->underlying().get();
        return this->underlying();
    }

    IGNORE_SHOULD_RETURN_REFERENCE_TO_THIS_END
};

template <typename T>
struct PostIncrementable : crtp<T, PostIncrementable>
{
    IGNORE_SHOULD_RETURN_REFERENCE_TO_THIS_BEGIN

    FLUENT_CONSTEXPR17 T operator++(int)
    {
        return T(this->underlying().get()++);
    }

    IGNORE_SHOULD_RETURN_REFERENCE_TO_THIS_END
};

template <typename T>
struct PreDecrementable : crtp<T, PreDecrementable>
{
    IGNORE_SHOULD_RETURN_REFERENCE_TO_THIS_BEGIN

    FLUENT_CONSTEXPR17 T& operator--()
    {
        --this->underlying().get();
        return this->underlying();
    }

    IGNORE_SHOULD_RETURN_REFERENCE_TO_THIS_END
};

template <typename T>
struct PostDecrementable : crtp<T, PostDecrementable>
{
    IGNORE_SHOULD_RETURN_REFERENCE_TO_THIS_BEGIN

    FLUENT_CONSTEXPR17 T operator--(int)
    {
        return T( this->underlying().get()-- );
    }

    IGNORE_SHOULD_RETURN_REFERENCE_TO_THIS_END
};

template <typename T>
struct BinaryAddable : crtp<T, BinaryAddable>
{
    FLUENT_NODISCARD constexpr T operator+(T const& other) const
    {
        return T(this->underlying().get() + other.get());
    }
    FLUENT_CONSTEXPR17 T& operator+=(T const& other)
    {
        this->underlying().get() += other.get();
        return this->underlying();
    }
};

template <typename T>
struct UnaryAddable : crtp<T, UnaryAddable>
{
    FLUENT_NODISCARD constexpr T operator+() const
    {
        return T(+this->underlying().get());
    }
};

template <typename T>
struct Addable : BinaryAddable<T>, UnaryAddable<T>
{
    using BinaryAddable<T>::operator+;
    using UnaryAddable<T>::operator+;
};

template <typename T>
struct BinarySubtractable : crtp<T, BinarySubtractable>
{
    FLUENT_NODISCARD constexpr T operator-(T const& other) const
    {
        return T(this->underlying().get() - other.get());
    }
    FLUENT_CONSTEXPR17 T& operator-=(T const& other)
    {
        this->underlying().get() -= other.get();
        return this->underlying();
    }
};
   
template <typename T>
struct UnarySubtractable : crtp<T, UnarySubtractable>
{
    FLUENT_NODISCARD constexpr T operator-() const
    {
        return T(-this->underlying().get());
    }
};
   
template <typename T>
struct Subtractable : BinarySubtractable<T>, UnarySubtractable<T>
{
    using UnarySubtractable<T>::operator-;
    using BinarySubtractable<T>::operator-;
};
   
template <typename T>
struct Multiplicable : crtp<T, Multiplicable>
{
    FLUENT_NODISCARD constexpr T operator*(T const& other) const
    {
        return T(this->underlying().get() * other.get());
    }
    FLUENT_CONSTEXPR17 T& operator*=(T const& other)
    {
        this->underlying().get() *= other.get();
        return this->underlying();
    }
};

template <typename T>
struct Divisible : crtp<T, Divisible>
{
    FLUENT_NODISCARD constexpr T operator/(T const& other) const
    {
        return T(this->underlying().get() / other.get());
    }
    FLUENT_CONSTEXPR17 T& operator/=(T const& other)
    {
        this->underlying().get() /= other.get();
        return this->underlying();
    }
};

template <typename T>
struct Modulable : crtp<T, Modulable>
{
    FLUENT_NODISCARD constexpr T operator%(T const& other) const
    {
        return T(this->underlying().get() % other.get());
    }
    FLUENT_CONSTEXPR17 T& operator%=(T const& other)
    {
        this->underlying().get() %= other.get();
        return this->underlying();
    }
};

template <typename T>
struct BitWiseInvertable : crtp<T, BitWiseInvertable>
{
    FLUENT_NODISCARD constexpr T operator~() const
    {
        return T(~this->underlying().get());
    }
};

template <typename T>
struct BitWiseAndable : crtp<T, BitWiseAndable>
{
    FLUENT_NODISCARD constexpr T operator&(T const& other) const
    {
        return T(this->underlying().get() & other.get());
    }
    FLUENT_CONSTEXPR17 T& operator&=(T const& other)
    {
        this->underlying().get() &= other.get();
        return this->underlying();
    }
};

template <typename T>
struct BitWiseOrable : crtp<T, BitWiseOrable>
{
    FLUENT_NODISCARD constexpr T operator|(T const& other) const
    {
        return T(this->underlying().get() | other.get());
    }
    FLUENT_CONSTEXPR17 T& operator|=(T const& other)
    {
        this->underlying().get() |= other.get();
        return this->underlying();
    }
};

template <typename T>
struct BitWiseXorable : crtp<T, BitWiseXorable>
{
    FLUENT_NODISCARD constexpr T operator^(T const& other) const
    {
        return T(this->underlying().get() ^ other.get());
    }
    FLUENT_CONSTEXPR17 T& operator^=(T const& other)
    {
        this->underlying().get() ^= other.get();
        return this->underlying();
    }
};

template <typename T>
struct BitWiseLeftShiftable : crtp<T, BitWiseLeftShiftable>
{
    FLUENT_NODISCARD constexpr T operator<<(T const& other) const
    {
        return T(this->underlying().get() << other.get());
    }
    FLUENT_CONSTEXPR17 T& operator<<=(T const& other)
    {
        this->underlying().get() <<= other.get();
        return this->underlying();
    }
};

template <typename T>
struct BitWiseRightShiftable : crtp<T, BitWiseRightShiftable>
{
    FLUENT_NODISCARD constexpr T operator>>(T const& other) const
    {
        return T(this->underlying().get() >> other.get());
    }
    FLUENT_CONSTEXPR17 T& operator>>=(T const& other)
    {
        this->underlying().get() >>= other.get();
        return this->underlying();
    }
};

template <typename T>
struct Comparable : crtp<T, Comparable>
{
    FLUENT_NODISCARD constexpr bool operator<(T const& other) const
    {
        return this->underlying().get() < other.get();
    }
    FLUENT_NODISCARD constexpr bool operator>(T const& other) const
    {
        return other.get() < this->underlying().get();
    }
    FLUENT_NODISCARD constexpr bool operator<=(T const& other) const
    {
        return !(other.get() < this->underlying().get());
    }
    FLUENT_NODISCARD constexpr bool operator>=(T const& other) const
    {
        return !(*this < other);
    }
// On Visual Studio before 19.22, you cannot define constexpr with friend function
// See: https://stackoverflow.com/a/60400110
#if defined(_MSC_VER) && _MSC_VER < 1922
    FLUENT_NODISCARD constexpr bool operator==(T const& other) const
    {
        return !(*this < other) && !(other.get() < this->underlying().get());
    }
#else
    FLUENT_NODISCARD friend constexpr bool operator==(Comparable<T> const& self, T const& other)
    {
        return !(self < other) && !(other.get() < self.underlying().get());
    }
#endif
    FLUENT_NODISCARD constexpr bool operator!=(T const& other) const
    {
        return !(*this == other);
    }
};

template< typename T >
struct Dereferencable;

template< typename T, typename Parameter, template< typename > class ... Skills >
struct Dereferencable<NamedType<T, Parameter, Skills...>> : crtp<NamedType<T, Parameter, Skills...>, Dereferencable>
{
    FLUENT_NODISCARD constexpr T& operator*() &
    {
        return this->underlying().get();
    }
    FLUENT_NODISCARD constexpr std::remove_reference_t<T> const& operator*() const &
    {
        return this->underlying().get();
    }
};

template <typename Destination>
struct ImplicitlyConvertibleTo
{
    template <typename T>
    struct templ : crtp<T, templ>
    {
        FLUENT_NODISCARD constexpr operator Destination() const
        {
            return this->underlying().get();
        }
    };
};

template <typename T>
struct Printable : crtp<T, Printable>
{
    static constexpr bool is_printable = true;

    void print(std::ostream& os) const
    {
        os << this->underlying().get();
    }
};

template <typename T, typename Parameter, template <typename> class... Skills>
typename std::enable_if<NamedType<T, Parameter, Skills...>::is_printable, std::ostream&>::type
operator<<(std::ostream& os, NamedType<T, Parameter, Skills...> const& object)
{
    object.print(os);
    return os;
}

template <typename T>
struct Hashable
{
    static constexpr bool is_hashable = true;
};

template <typename NamedType_>
struct FunctionCallable;

template <typename T, typename Parameter, template <typename> class... Skills>
struct FunctionCallable<NamedType<T, Parameter, Skills...>> : crtp<NamedType<T, Parameter, Skills...>, FunctionCallable>
{
    FLUENT_NODISCARD constexpr operator T const&() const
    {
        return this->underlying().get();
    }
    FLUENT_NODISCARD constexpr operator T&()
    {
        return this->underlying().get();
    }
};

template <typename NamedType_>
struct MethodCallable;

template <typename T, typename Parameter, template <typename> class... Skills>
struct MethodCallable<NamedType<T, Parameter, Skills...>> : crtp<NamedType<T, Parameter, Skills...>, MethodCallable>
{
    FLUENT_NODISCARD FLUENT_CONSTEXPR17 std::remove_reference_t<T> const* operator->() const
    {
        return std::addressof(this->underlying().get());
    }
    FLUENT_NODISCARD FLUENT_CONSTEXPR17 std::remove_reference_t<T>* operator->()
    {
        return std::addressof(this->underlying().get());
    }
};

template <typename NamedType_>
struct Callable
    : FunctionCallable<NamedType_>
    , MethodCallable<NamedType_>
{
};

template <typename T>
struct Arithmetic
    : PreIncrementable<T>
    , PostIncrementable<T>
    , PreDecrementable<T>
    , PostDecrementable<T>
    , Addable<T>
    , Subtractable<T>
    , Multiplicable<T>
    , Divisible<T>
    , Modulable<T>
    , BitWiseInvertable<T>
    , BitWiseAndable<T>
    , BitWiseOrable<T>
    , BitWiseXorable<T>
    , BitWiseLeftShiftable<T>
    , BitWiseRightShiftable<T>
    , Comparable<T>
    , Printable<T>
    , Hashable<T>
{
};

} // namespace fluent

namespace std
{
template <typename T, typename Parameter, template <typename> class... Skills>
struct hash<fluent::NamedType<T, Parameter, Skills...>>
{
    using NamedType = fluent::NamedType<T, Parameter, Skills...>;
    using checkIfHashable = typename std::enable_if<NamedType::is_hashable, void>::type;

    size_t operator()(fluent::NamedType<T, Parameter, Skills...> const& x) const noexcept
    {
        static_assert(noexcept(std::hash<T>()(x.get())), "hash fuction should not throw");

        return std::hash<T>()(x.get());
    }
};

} // namespace std

#endif
