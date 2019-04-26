#pragma once

namespace sqlite_orm {
    
    /**
     *  Specialization for optional type (std::shared_ptr / std::unique_ptr).
     */
    template <typename T>
    struct is_std_ptr : std::false_type {};
    
    template <typename T>
    struct is_std_ptr<std::shared_ptr<T>> : std::true_type {
        static std::shared_ptr<T> make(const T& v) {
            return std::make_shared<T>(v);
        }
    };
    
    template <typename T>
    struct is_std_ptr<std::unique_ptr<T>> : std::true_type {
        static std::unique_ptr<T> make(const T& v) {
            return std::make_unique<T>(v);
        }
    };
}
