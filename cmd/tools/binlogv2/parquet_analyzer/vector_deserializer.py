"""
Vector Deserializer Component
Responsible for deserializing vector data in parquet files
References deserialization logic from serde.go
"""

import struct
import numpy as np
from typing import List, Optional, Union, Dict, Any


class VectorDeserializer:
    """Vector deserializer"""
    
    @staticmethod
    def detect_vector_type_and_dim(bytes_data: bytes, field_name: str = "") -> tuple[str, int]:
        """
        Detect vector type and dimension based on field name and byte data
        
        Args:
            bytes_data: byte data
            field_name: field name
            
        Returns:
            tuple: (vector_type, dimension)
        """
        if not bytes_data:
            return "Unknown", 0
        
        # First, try to detect if it's JSON data
        try:
            # Check if it starts with { or [ (JSON indicators)
            if bytes_data.startswith(b'{') or bytes_data.startswith(b'['):
                return "JSON", len(bytes_data)
            
            # Try to decode as UTF-8 and check if it's valid JSON
            decoded = bytes_data.decode('utf-8', errors='ignore')
            if decoded.startswith('{') or decoded.startswith('['):
                return "JSON", len(bytes_data)
        except:
            pass
        
        # Check for specific field name patterns
        if "json" in field_name.lower():
            return "JSON", len(bytes_data)
        
        # Check for array patterns (Protocol Buffers style)
        if "array" in field_name.lower():
            # Check if it looks like a protobuf array
            if len(bytes_data) > 2 and bytes_data[1] == ord('-') and b'\n\x01' in bytes_data:
                return "Array", len(bytes_data)
        
        # Infer type based on field name
        if "vector" in field_name.lower():
            # For vector fields, prioritize checking if it's Int8Vector (one uint8 per byte)
            if len(bytes_data) <= 256:  # Usually vector dimension won't exceed 256
                return "Int8Vector", len(bytes_data)
            elif len(bytes_data) % 4 == 0:
                dim = len(bytes_data) // 4
                return "FloatVector", dim
            elif len(bytes_data) % 2 == 0:
                dim = len(bytes_data) // 2
                if "float16" in field_name.lower():
                    return "Float16Vector", dim
                elif "bfloat16" in field_name.lower():
                    return "BFloat16Vector", dim
                else:
                    return "Float16Vector", dim  # default
            else:
                # Binary vector, calculate dimension
                dim = len(bytes_data) * 8
                return "BinaryVector", dim
        else:
            # Infer based on byte count
            if len(bytes_data) % 4 == 0:
                dim = len(bytes_data) // 4
                return "FloatVector", dim
            elif len(bytes_data) % 2 == 0:
                dim = len(bytes_data) // 2
                return "Float16Vector", dim
            else:
                dim = len(bytes_data) * 8
                return "BinaryVector", dim
    
    @staticmethod
    def deserialize_float_vector(bytes_data: bytes, dim: Optional[int] = None) -> Optional[List[float]]:
        """
        Deserialize FloatVector
        References FloatVector processing logic from serde.go
        
        Args:
            bytes_data: byte data
            dim: dimension, if None will auto-calculate
            
        Returns:
            List[float]: deserialized float vector
        """
        if not bytes_data:
            return None
        
        try:
            if dim is None:
                dim = len(bytes_data) // 4
            
            if len(bytes_data) != dim * 4:
                raise ValueError(f"FloatVector size mismatch: expected {dim * 4}, got {len(bytes_data)}")
            
            # Use struct to unpack float32 data
            floats = struct.unpack(f'<{dim}f', bytes_data)
            return list(floats)
        
        except Exception as e:
            print(f"FloatVector deserialization failed: {e}")
            return None
    
    @staticmethod
    def deserialize_binary_vector(bytes_data: bytes, dim: Optional[int] = None) -> Optional[List[int]]:
        """
        Deserialize BinaryVector
        References BinaryVector processing logic from serde.go
        
        Args:
            bytes_data: byte data
            dim: dimension, if None will auto-calculate
            
        Returns:
            List[int]: deserialized binary vector
        """
        if not bytes_data:
            return None
        
        try:
            if dim is None:
                dim = len(bytes_data) * 8
            
            expected_size = (dim + 7) // 8
            if len(bytes_data) != expected_size:
                raise ValueError(f"BinaryVector size mismatch: expected {expected_size}, got {len(bytes_data)}")
            
            # Convert to binary vector
            binary_vector = []
            for byte in bytes_data:
                for i in range(8):
                    bit = (byte >> i) & 1
                    binary_vector.append(bit)
            
            # Only return first dim elements
            return binary_vector[:dim]
        
        except Exception as e:
            print(f"BinaryVector deserialization failed: {e}")
            return None
    
    @staticmethod
    def deserialize_int8_vector(bytes_data: bytes, dim: Optional[int] = None) -> Optional[List[int]]:
        """
        Deserialize Int8Vector
        References Int8Vector processing logic from serde.go
        
        Args:
            bytes_data: byte data
            dim: dimension, if None will auto-calculate
            
        Returns:
            List[int]: deserialized int8 vector
        """
        if not bytes_data:
            return None
        
        try:
            if dim is None:
                dim = len(bytes_data)
            
            if len(bytes_data) != dim:
                raise ValueError(f"Int8Vector size mismatch: expected {dim}, got {len(bytes_data)}")
            
            # Convert to int8 array
            int8_vector = [int8 for int8 in bytes_data]
            return int8_vector
        
        except Exception as e:
            print(f"Int8Vector deserialization failed: {e}")
            return None
    
    @staticmethod
    def deserialize_float16_vector(bytes_data: bytes, dim: Optional[int] = None) -> Optional[List[float]]:
        """
        Deserialize Float16Vector
        References Float16Vector processing logic from serde.go
        
        Args:
            bytes_data: byte data
            dim: dimension, if None will auto-calculate
            
        Returns:
            List[float]: deserialized float16 vector
        """
        if not bytes_data:
            return None
        
        try:
            if dim is None:
                dim = len(bytes_data) // 2
            
            if len(bytes_data) != dim * 2:
                raise ValueError(f"Float16Vector size mismatch: expected {dim * 2}, got {len(bytes_data)}")
            
            # Convert to float16 array
            float16_vector = []
            for i in range(0, len(bytes_data), 2):
                if i + 1 < len(bytes_data):
                    # Simple float16 conversion (simplified here)
                    uint16 = struct.unpack('<H', bytes_data[i:i+2])[0]
                    # Convert to float32 (simplified version)
                    float_val = float(uint16) / 65535.0  # normalization
                    float16_vector.append(float_val)
            
            return float16_vector
        
        except Exception as e:
            print(f"Float16Vector deserialization failed: {e}")
            return None
    
    @staticmethod
    def deserialize_bfloat16_vector(bytes_data: bytes, dim: Optional[int] = None) -> Optional[List[float]]:
        """
        Deserialize BFloat16Vector
        References BFloat16Vector processing logic from serde.go
        
        Args:
            bytes_data: byte data
            dim: dimension, if None will auto-calculate
            
        Returns:
            List[float]: deserialized bfloat16 vector
        """
        if not bytes_data:
            return None
        
        try:
            if dim is None:
                dim = len(bytes_data) // 2
            
            if len(bytes_data) != dim * 2:
                raise ValueError(f"BFloat16Vector size mismatch: expected {dim * 2}, got {len(bytes_data)}")
            
            # Convert to bfloat16 array
            bfloat16_vector = []
            for i in range(0, len(bytes_data), 2):
                if i + 1 < len(bytes_data):
                    # Simple bfloat16 conversion (simplified here)
                    uint16 = struct.unpack('<H', bytes_data[i:i+2])[0]
                    # Convert to float32 (simplified version)
                    float_val = float(uint16) / 65535.0  # normalization
                    bfloat16_vector.append(float_val)
            
            return bfloat16_vector
        
        except Exception as e:
            print(f"BFloat16Vector deserialization failed: {e}")
            return None
    
    @staticmethod
    def deserialize_json(bytes_data: bytes) -> Optional[Dict[str, Any]]:
        """
        Deserialize JSON data
        
        Args:
            bytes_data: byte data
            
        Returns:
            Dict[str, Any]: deserialized JSON object
        """
        if not bytes_data:
            return None
        
        try:
            import json
            # Try to decode as UTF-8
            decoded = bytes_data.decode('utf-8')
            return json.loads(decoded)
        except UnicodeDecodeError:
            try:
                # Try with different encoding
                decoded = bytes_data.decode('latin-1')
                return json.loads(decoded)
            except:
                pass
        except json.JSONDecodeError as e:
            print(f"JSON deserialization failed: {e}")
            return None
        except Exception as e:
            print(f"JSON deserialization failed: {e}")
            return None
    
    @staticmethod
    def deserialize_array(bytes_data: bytes) -> Optional[List[str]]:
        """
        Deserialize array data (Protocol Buffers style)
        
        Args:
            bytes_data: byte data
            
        Returns:
            List[str]: deserialized array
        """
        if not bytes_data:
            return None
        
        try:
            # Parse protobuf-style array format
            # Format: length + separator + data
            # Example: b'2-\n\x01q\n\x01k\n\x01l...'
            
            # Skip the first few bytes (length and separator)
            if len(bytes_data) < 3:
                return None
            
            # Find the start of data (after the first separator)
            start_idx = 0
            for i in range(len(bytes_data) - 1):
                if bytes_data[i:i+2] == b'\n\x01':
                    start_idx = i + 2
                    break
            
            if start_idx == 0:
                return None
            
            # Extract the data part
            data_part = bytes_data[start_idx:]
            
            # Split by separator and decode
            array_items = []
            
            # Split by \n\x01 separator
            parts = data_part.split(b'\n\x01')
            
            for part in parts:
                if part:  # Skip empty parts
                    try:
                        # Each part should be a single character
                        if len(part) == 1:
                            char = part.decode('utf-8')
                            array_items.append(char)
                    except UnicodeDecodeError:
                        try:
                            char = part.decode('latin-1')
                            array_items.append(char)
                        except:
                            pass
            
            return array_items
            
        except Exception as e:
            print(f"Array deserialization failed: {e}")
            return None
    
    @staticmethod
    def deserialize_vector(bytes_data: bytes, vector_type: str, dim: Optional[int] = None) -> Optional[Union[List[float], List[int], Dict[str, Any]]]:
        """
        Generic vector deserialization method
        
        Args:
            bytes_data: byte data
            vector_type: vector type
            dim: dimension, if None will auto-calculate
            
        Returns:
            Union[List[float], List[int], Dict[str, Any]]: deserialized vector
        """
        if not bytes_data:
            return None
        
        try:
            if vector_type == "FloatVector":
                return VectorDeserializer.deserialize_float_vector(bytes_data, dim)
            elif vector_type == "BinaryVector":
                return VectorDeserializer.deserialize_binary_vector(bytes_data, dim)
            elif vector_type == "Int8Vector":
                return VectorDeserializer.deserialize_int8_vector(bytes_data, dim)
            elif vector_type == "Float16Vector":
                return VectorDeserializer.deserialize_float16_vector(bytes_data, dim)
            elif vector_type == "BFloat16Vector":
                return VectorDeserializer.deserialize_bfloat16_vector(bytes_data, dim)
            elif vector_type == "JSON":
                return VectorDeserializer.deserialize_json(bytes_data)
            elif vector_type == "Array":
                return VectorDeserializer.deserialize_array(bytes_data)
            else:
                # Other binary types, return hex representation of original bytes
                return bytes_data.hex()
        
        except Exception as e:
            print(f"Vector deserialization failed: {e}")
            return bytes_data.hex()
    
    @staticmethod
    def analyze_vector_statistics(vector_data: Union[List[float], List[int], Dict[str, Any]], vector_type: str) -> Dict[str, Any]:
        """
        Analyze vector statistics
        
        Args:
            vector_data: vector data
            vector_type: vector type
            
        Returns:
            Dict: statistics information
        """
        if not vector_data:
            return {}
        
        try:
            if vector_type == "JSON":
                # For JSON data, return basic info
                if isinstance(vector_data, dict):
                    return {
                        "vector_type": vector_type,
                        "keys": list(vector_data.keys()),
                        "key_count": len(vector_data),
                        "is_object": True
                    }
                elif isinstance(vector_data, list):
                    return {
                        "vector_type": vector_type,
                        "length": len(vector_data),
                        "is_array": True
                    }
                else:
                    return {
                        "vector_type": vector_type,
                        "value_type": type(vector_data).__name__
                    }
            elif vector_type == "Array":
                # For Array data, return basic info
                if isinstance(vector_data, list):
                    return {
                        "vector_type": vector_type,
                        "length": len(vector_data),
                        "is_array": True,
                        "item_types": list(set(type(item).__name__ for item in vector_data))
                    }
                else:
                    return {
                        "vector_type": vector_type,
                        "value_type": type(vector_data).__name__
                    }
            
            # For numeric vectors
            array_data = np.array(vector_data)
            
            stats = {
                "vector_type": vector_type,
                "dimension": len(vector_data),
                "min": float(array_data.min()),
                "max": float(array_data.max()),
                "mean": float(array_data.mean()),
                "std": float(array_data.std())
            }
            
            if vector_type == "BinaryVector":
                stats["zero_count"] = int(np.sum(array_data == 0))
                stats["one_count"] = int(np.sum(array_data == 1))
            
            return stats
        
        except Exception as e:
            print(f"Statistics calculation failed: {e}")
            return {}
    
    @staticmethod
    def analyze_vector_pattern(bytes_data: bytes) -> Dict[str, Any]:
        """
        Analyze vector data patterns
        
        Args:
            bytes_data: byte data
            
        Returns:
            Dict: pattern analysis results
        """
        if not bytes_data:
            return {}
        
        try:
            # Convert to integer array
            int_data = list(bytes_data)
            
            # Check if it's an increasing pattern
            is_increasing = all(int_data[i] <= int_data[i+1] for i in range(len(int_data)-1))
            is_decreasing = all(int_data[i] >= int_data[i+1] for i in range(len(int_data)-1))
            
            # Check if it's a cyclic pattern (i + j) % 256
            is_cyclic = True
            for i in range(min(10, len(int_data))):  # Check first 10 elements
                expected = (i) % 256
                if int_data[i] != expected:
                    is_cyclic = False
                    break
            
            # Check if it's a sequential pattern [start, start+1, start+2, ...]
            is_sequential = True
            if len(int_data) > 1:
                start_val = int_data[0]
                for i in range(1, min(10, len(int_data))):
                    if int_data[i] != start_val + i:
                        is_sequential = False
                        break
            else:
                is_sequential = False
            
            # Calculate statistics
            unique_values = len(set(int_data))
            min_val = min(int_data)
            max_val = max(int_data)
            avg_val = sum(int_data) / len(int_data)
            
            return {
                "is_increasing": is_increasing,
                "is_decreasing": is_decreasing,
                "is_cyclic": is_cyclic,
                "is_sequential": is_sequential,
                "unique_values": unique_values,
                "min_value": min_val,
                "max_value": max_val,
                "avg_value": avg_val,
                "pattern_type": "sequential" if is_sequential else "cyclic" if is_cyclic else "increasing" if is_increasing else "decreasing" if is_decreasing else "random"
            }
        
        except Exception as e:
            print(f"Pattern analysis failed: {e}")
            return {}
    
    @staticmethod
    def deserialize_with_analysis(bytes_data: bytes, field_name: str = "") -> Dict[str, Any]:
        """
        Deserialize and analyze vector data
        
        Args:
            bytes_data: byte data
            field_name: field name
            
        Returns:
            Dict: dictionary containing deserialization results and statistics
        """
        if not bytes_data:
            return {}
        
        # Detect vector type and dimension
        vector_type, dim = VectorDeserializer.detect_vector_type_and_dim(bytes_data, field_name)
        
        # Analyze data patterns
        pattern_analysis = VectorDeserializer.analyze_vector_pattern(bytes_data)
        
        # Deserialize
        deserialized_data = VectorDeserializer.deserialize_vector(bytes_data, vector_type, dim)
        
        # Analyze statistics
        stats = VectorDeserializer.analyze_vector_statistics(deserialized_data, vector_type)
        
        return {
            "raw_hex": bytes_data.hex(),
            "vector_type": vector_type,
            "dimension": dim,
            "deserialized": deserialized_data,
            "statistics": stats,
            "pattern_analysis": pattern_analysis
        } 