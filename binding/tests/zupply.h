/* Doxygen main page */
/*! \mainpage Zupply
###   A light-weight portable C++ 11 library for researches and demos

*   Author: Joshua Zhang
*   Date since: June-2015
*
*   Copyright (c) <2015> <JOSHUA Z. ZHANG>
*
*   Open source according to MIT License.
*
*   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
*   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
*   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
*   IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
*   CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
*   TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
*   SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*
*
*
#### Project on Github: [link](https://github.com/ZhreShold/zupply)
***************************************************************************/




#ifndef _ZUPPLY_ZUPPLY_HPP_
#define _ZUPPLY_ZUPPLY_HPP_

///////////////////////////////////////////////////////////////
// Require C++ 11 features
///////////////////////////////////////////////////////////////
#if ((defined(_MSC_VER) && _MSC_VER >= 1800) || __cplusplus >= 201103L)
// VC++ defined an older version of __cplusplus, but it should work later than vc12
#else
#error C++11 required, add -std=c++11 to CFLAG.
#endif

#ifdef _MSC_VER

// Disable silly security warnings
//#ifndef _CRT_SECURE_NO_WARNINGS
//#define _CRT_SECURE_NO_WARNINGS
//#endif

// Define __func__ macro
#ifndef __func__
#define __func__ __FUNCTION__
#endif

// Define NDEBUG in Release Mode, ensure assert() disabled.
#if (!defined(_DEBUG) && !defined(NDEBUG))
#define NDEBUG
#endif

#if _MSC_VER < 1900
#define ZUPPLY_NOEXCEPT throw()
#endif

#endif

#ifndef ZUPPLY_NOEXCEPT
#define ZUPPLY_NOEXCEPT noexcept
#endif

// Optional header only mode, not done.
#ifdef ZUPPLY_HEADER_ONLY
#define ZUPPLY_EXPORT inline
#else
#define ZUPPLY_EXPORT
#endif

#include <string>
#include <exception>
#include <fstream>
#include <utility>
#include <iostream>
#include <vector>
#include <locale>
#include <ctime>
#include <chrono>
#include <thread>
#include <mutex>
#include <atomic>
#include <map>
#include <unordered_map>
#include <memory>
#include <type_traits>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <functional>
#include <climits>
#include <cassert>
#include <cstring>
#include <cmath>


/*!
 * \namespace zz
 * \brief Namespace for zupply
 */
namespace zz
{
	/*!
		* \namespace	zz::consts
		*
		* \brief	Namespace for parameters.
		*/
	namespace consts
	{
		static const char* kExceptionPrefixGeneral = "[Zupply Exception] ";
		static const char* kExceptionPrefixLogic = "[Zupply Exception->Logic] ";
		static const char* kExceptionPrefixArgument = "[Zupply Exception->Logic->Argument] ";
		static const char* kExceptionPrefixRuntime = "[Zupply Exception->Runtime] ";
		static const char* kExceptionPrefixIO = "[Zupply Exception->Runtime->IO] ";
		static const char* kExceptionPrefixCast = "[Zupply Exception->Runtime->Cast] ";
		static const char* kExceptionPrefixMemory = "[Zupply Exception->Runtime->Memory] ";
		static const char* kExceptionPrefixStrictWarn = "[Zupply Exception->StrictWarn] ";
	}

	/*!
	* \class	UnCopyable
	*
	* \brief	A not copyable base class, should be inheritated privately.
	*/
	class UnCopyable
	{
	public:
		UnCopyable() {};
		// no copy constructor
		UnCopyable(const UnCopyable&) = delete;
		// no assignment
		UnCopyable& operator=(const UnCopyable&) = delete;
	};

	/*!
	* \class	UnMovable
	*
	* \brief	A not movable/copyable base class, should be inheritated privately.
	*/
	class UnMovable
	{
	public:
		UnMovable() {};
		// no copy constructor
		UnMovable(const UnMovable&) = delete;
		// no copy operator
		UnMovable& operator=(const UnMovable&) = delete;
		// no move constructor
		UnMovable(UnMovable&&) = delete;
		// no move operator
		UnMovable& operator=(UnMovable&&) = delete;
	};

	/*!
	* \class	Exception
	*
	* \brief	An exception with customized prefix information.
	*/
	class Exception : public std::exception
	{
	public:
		explicit Exception(const char* message, const char* prefix = consts::kExceptionPrefixGeneral)
		{
			message_ = std::string(prefix) + message;
		};
		explicit Exception(const std::string message, const char* prefix = consts::kExceptionPrefixGeneral)
		{
			message_ = std::string(prefix) + message;
		};
		virtual ~Exception() ZUPPLY_NOEXCEPT{};

		const char* what() const ZUPPLY_NOEXCEPT{ return message_.c_str(); };
	private:
		std::string message_;
	};

	/*!
	* \class	LogicException
	*
	* \brief	Exception for signalling logic errors.
	*/
	class LogicException : public Exception
	{
	public:
		explicit LogicException(const char *message) : Exception(message, consts::kExceptionPrefixLogic){};
		explicit LogicException(const std::string &message) : Exception(message, consts::kExceptionPrefixLogic){};
	};

	/*!
	* \class	ArgException
	*
	* \brief	Exception for signalling argument errors.
	*/
	class ArgException : public Exception
	{
	public:
		explicit ArgException(const char *message) : Exception(message, consts::kExceptionPrefixArgument){};
		explicit ArgException(const std::string &message) : Exception(message, consts::kExceptionPrefixArgument){};
	};

	/*!
	* \class	RuntimeException
	*
	* \brief	Exception for signalling unexpected runtime errors.
	*/
	class RuntimeException : public Exception
	{
	public:
		explicit RuntimeException(const char *message) : Exception(message, consts::kExceptionPrefixRuntime){};
		explicit RuntimeException(const std::string &message) : Exception(message, consts::kExceptionPrefixRuntime){};
	};

	/*!
	* \class	CastException
	*
	* \brief	Exception for signalling unsuccessful cast operations.
	*/
	class CastException : public Exception
	{
	public:
		explicit CastException(const char *message) : Exception(message, consts::kExceptionPrefixCast){};
		explicit CastException(const std::string &message) : Exception(message, consts::kExceptionPrefixCast){};
	};

	/*!
	* \class	IOException
	*
	* \brief	Exception for signalling unexpected IO errors.
	*/
	class IOException : public Exception
	{
	public:
		explicit IOException(const char *message) : Exception(message, consts::kExceptionPrefixIO){};
		explicit IOException(const std::string &message) : Exception(message, consts::kExceptionPrefixIO){};
	};

	/*!
	* \class	MemException
	*
	* \brief	Exception for signalling memory errors.
	*/
	class MemException : public Exception
	{
	public:
		explicit MemException(const char *message) : Exception(message, consts::kExceptionPrefixMemory){};
		explicit MemException(const std::string &message) : Exception(message, consts::kExceptionPrefixMemory){};
	};

	/*!
	* \class	WarnException
	*
	* \brief	Exception for signalling warning errors when strict warning is enabled.
	*/
	class WarnException : public Exception
	{
	public:
		explicit WarnException(const char *message) : Exception(message, consts::kExceptionPrefixStrictWarn){};
		explicit WarnException(const std::string &message) : Exception(message, consts::kExceptionPrefixStrictWarn){};
	};

	/*!
	 * \namespace  zz::math
	 * \brief Namespace for math operations
	 */
	namespace math
	{
		using std::min;
		using std::max;
		using std::abs;

		/*!
		 * \fn template<class T> inline const T clip(const T& value, const T& low, const T& high)
		 * \brief Clip values in-between low and high values
		 * \param value The value to be clipped
		 * \param low The lower bound
		 * \param high The higher bound
		 * \return Value if value in[low, high] or low if (value < low) or high if (value > high)
		 * \note Foolproof if low/high are not set correctly
		 */
		template<class T> inline const T clip(const T& value, const T& low, const T& high)
		{
			// fool proof max/min values
			T h = (std::max)(low, high);
			T l = (std::min)(low, high);
			return (std::max)((std::min)(value, h), l);
		}

		/*!
		 * \brief Template meta programming for pow(a, b) where a, b must be natural numbers
		 * Use Pow<a, b>::result = a^b, which is computed in compilation rather than runtime.
		 */
		template <unsigned long B, unsigned long E>
		struct Pow
		{
			static const unsigned long result = B * Pow<B, E - 1>::result;
		};

		template <unsigned long B>
		struct Pow<B, 0>
		{
			static const unsigned long result = 1;
		};


		/*!
		 * \fn int round(double value)
		 * \brief fast round utilizing hardware acceleration features
		 * \param value
		 * \return Rounded int
		 */
		inline int round(double value)
		{
#if ((defined _MSC_VER && defined _M_X64) || (defined __GNUC__ && defined __x86_64__ && defined __SSE2__ && !defined __APPLE__)) && !defined(__CUDACC__) && 0
			__m128d t = _mm_set_sd(value);
			return _mm_cvtsd_si32(t);
#elif defined _MSC_VER && defined _M_IX86
			int t;
			__asm
			{
				fld value;
				fistp t;
			}
			return t;
#elif defined _MSC_VER && defined _M_ARM && defined HAVE_TEGRA_OPTIMIZATION
			TEGRA_ROUND(value);
#elif defined CV_ICC || defined __GNUC__
#  ifdef HAVE_TEGRA_OPTIMIZATION
			TEGRA_ROUND(value);
#  else
			return (int)lrint(value);
#  endif
#else
			double intpart, fractpart;
			fractpart = modf(value, &intpart);
			if ((fabs(fractpart) != 0.5) || ((((int)intpart) % 2) != 0))
				return (int)(value + (value >= 0 ? 0.5 : -0.5));
			else
				return (int)intpart;
#endif
		}

	} // namespace math

	/*!
	 * \namespace zz::misc
	 * \brief Namespace for miscellaneous utility functions
	 */
	namespace misc
	{
		/*!
		 * \fn inline void unused(const T&)
		 * \brief Suppress warning for unused variables, do nothing actually
		 */
		template<typename T>
		inline void unused(const T&) {}

		namespace detail
		{
			// To allow ADL with custom begin/end
			using std::begin;
			using std::end;

			template <typename T>
			auto is_iterable_impl(int)
				-> decltype (
				begin(std::declval<T&>()) != end(std::declval<T&>()), // begin/end and operator !=
				++std::declval<decltype(begin(std::declval<T&>()))&>(), // operator ++
				*begin(std::declval<T&>()), // operator*
				std::true_type{});

			template <typename T>
			std::false_type is_iterable_impl(...);

		}

		template <typename T>
		using is_iterable = decltype(detail::is_iterable_impl<T>(0));

		/*!
		 * \brief The general functor Callback class
		 */
		class Callback
		{
		public:
			Callback(std::function<void()> f) : f_(f) { f_(); }
		private:
			std::function<void()> f_;
		};
	} // namespace misc

	/*!
	 * \namespace zz::cds
	 * \brief Namespace for concurrent data structures
	 */
	namespace cds
	{
		inline void nop_pause()
		{
#if (defined _MSC_VER) && (defined _M_IX86 || defined _M_X64) && 0
			__asm volatile("pause" ::: "memory");
#endif
		}

		/*!
		* \class	NullMutex
		*
		* \brief	A null mutex, no cost.
		*/
		class NullMutex
		{
		public:
			void lock() {}
			void unlock() {}
			bool try_lock() { return true; }
		};

		/*!
		* \class	SpinLock
		*
		* \brief	A simple spin lock utilizing c++11 atomic_flag
		*/
		class SpinLock : UnMovable
		{
		public:
			SpinLock(){ flag_.clear(); }
			inline void lock()
			{
				while (flag_.test_and_set(std::memory_order_acquire))
				{
					nop_pause();	// no op or use architecture pause
				}
			}

			inline void unlock()
			{
				flag_.clear(std::memory_order_release);
			}

			inline bool try_lock()
			{
				return !flag_.test_and_set(std::memory_order_acquire);
			}
		private:
			std::atomic_flag flag_;
		};

		class RWLockable;

		class RWLock {
			friend class RWLockable;

		public:
			enum class LockType {
				none,
				read,
				write
			};

		private:
			RWLockable * lockable_;
			LockType lockType_;

			RWLock(RWLockable * const lockable, bool const exclusive);
			RWLock();

		public:
			RWLock(RWLock&& rhs);
			RWLock& operator =(RWLock&& rhs);
			~RWLock();

			void unlock();
			LockType get_lock_type() const;
		};

		class RWLockable {
			friend RWLock::~RWLock();

		private:
			class Counters {
			private:
				uint16_t read_;
				uint8_t writeClaim_;
				uint8_t writeDone_;
			public:
				bool is_waiting_for_write() const;
				bool is_waiting_for_read() const;
				bool is_my_turn_to_write(Counters const & claim) const;

				bool want_to_read(Counters * buf) const;
				bool want_to_write(Counters * buf) const;
				Counters done_reading() const;
				Counters done_writing() const;
			};

			std::atomic<Counters> counters_;

			void unlock_read();
			void unlock_write();

		public:
			RWLock lock_for_read();
			RWLock lock_for_write();
			bool is_lock_free() const;
		};

		namespace lockfree
		{
			template <typename Key, typename Value> class UnorderedMap : public UnMovable
			{
			public:
				using MapType = std::unordered_map<Key, Value>;
				using MapPairType = std::pair<Key, Value>;

				UnorderedMap() {}

				bool is_lock_free() const
				{
					return lockable_.is_lock_free();
				}

				bool contains(const Key& key)
				{
					auto lock = lockable_.lock_for_read();
					return map_.count(key) > 0;
				}

				bool get(const Key& key, Value& dst)
				{
					auto lock = lockable_.lock_for_read();
					if (map_.count(key) > 0)
					{
						dst = map_[key];
						return true;
					}
					return false;
				}

				MapType snapshot()
				{
					auto lock = lockable_.lock_for_read();
					return map_;
				}

				bool insert(const Key& key, const Value& value)
				{
					if (contains(key)) return false;
					auto lock = lockable_.lock_for_write();
					map_[key] = value;
					return true;
				}

				bool insert(const MapPairType& pair)
				{
					return insert(pair.first, pair.second);
				}

				void replace(const Key& key, const Value& value)
				{
					auto lock = lockable_.lock_for_write();
					map_[key] = value;
				}

				void replace(const MapPairType& pair)
				{
					replace(pair.first, pair.second);
				}

				void erase(const Key& key)
				{
					auto lock = lockable_.lock_for_write();
					map_.erase(key);
				}

				void clear()
				{
					auto lock = lockable_.lock_for_write();
					map_.clear();
				}

				//template <typename Function>
				//void for_each(std::function<Function> functor)
				//{
				//	auto lock = lockable_.lock_for_read();
				//	std::for_each(map_.begin(), map_.end(), functor);
				//}

			private:

				RWLockable	lockable_;
				MapType	map_;
			};

			template <typename T> class NonTrivialContainer : public UnMovable
			{
			public:
				NonTrivialContainer() {}

				bool is_lock_free() const
				{
					return lockable_.is_lock_free();
				}

				T get()
				{
					auto lock = lockable_.lock_for_read();
					return obj_;
				}

				void set(const T& t)
				{
					//std::lock_guard<std::mutex> lock(mutex_);
					auto lock = lockable_.lock_for_write();
					obj_ = t;
				}

			private:
				T	obj_;
				RWLockable lockable_;

			};
		}

		namespace lockbased
		{
			template <typename Key, typename Value> class UnorderedMap : public UnMovable
			{
			public:
				using MapType = std::unordered_map<Key, Value>;
				using MapPairType = std::pair<Key, Value>;

				UnorderedMap() {}

				bool is_lock_free() const
				{
					return false;
				}

				bool contains(const Key& key)
				{
					std::lock_guard<std::mutex> lock(mutex_);
					return map_.count(key) > 0;
				}

				bool get(const Key& key, Value& dst)
				{
					std::lock_guard<std::mutex> lock(mutex_);
					if (map_.count(key) > 0)
					{
						dst = map_[key];
						return true;
					}
					return false;
				}

				MapType snapshot()
				{
					std::lock_guard<std::mutex> lock(mutex_);
					return map_;
				}

				bool insert(const Key& key, const Value& value)
				{
					if (contains(key)) return false;
					std::lock_guard<std::mutex> lock(mutex_);
					map_[key] = value;
					return true;
				}

				bool insert(const MapPairType& pair)
				{
					return insert(pair.first, pair.second);
				}

				void replace(const Key& key, const Value& value)
				{
					std::lock_guard<std::mutex> lock(mutex_);
					map_[key] = value;
				}

				void replace(const MapPairType& pair)
				{
					replace(pair.first, pair.second);
				}

				void erase(const Key& key)
				{
					std::lock_guard<std::mutex> lock(mutex_);
					map_.erase(key);
				}

				void clear()
				{
					std::lock_guard<std::mutex> lock(mutex_);
					map_.clear();
				}

				//template <typename Function>
				//void for_each(std::function<Function> functor)
				//{
				//	auto lock = lockable_.lock_for_read();
				//	std::for_each(map_.begin(), map_.end(), functor);
				//}

			private:

				std::mutex mutex_;
				MapType	map_;
			};

			template <typename T> class NonTrivialContainer : public UnMovable
			{
			public:
				NonTrivialContainer() {}

				bool is_lock_free() const
				{
					return false;
				}

				T get()
				{
					std::lock_guard<std::mutex> lock(mutex_);
					return obj_;
				}

				void set(const T& t)
				{
					std::lock_guard<std::mutex> lock(mutex_);
					obj_ = t;
				}

			private:
				T	obj_;
				std::mutex mutex_;

			};
		}

		///*!
		// * \brief AtomicUnorderedMap Template atomic unordered_map<>
		// * AtomicUnorderedMap is lock-free, however, modification will create copies.
		// * Thus this structure is good for read-many write-rare purposes.
		// */
		//template <typename Key, typename Value> class AtomicUnorderedMap
		//{
		//	using MapType = std::unordered_map<Key, Value>;
		//	using MapPtr = std::shared_ptr<MapType>;
		//public:
		//	AtomicUnorderedMap()
		//	{
		//		mapPtr_ = std::make_shared<MapType>();
		//	}

		//	/*!
		//	 * \brief Get shared_ptr of unorderd_map instance
		//	 * \return Shared_ptr of unordered_map
		//	 */
		//	MapPtr get()
		//	{
		//		return std::atomic_load(&mapPtr_);
		//	}

		//	/*!
		//	 * \brief Insert key-value pair to the map
		//	 * \param key
		//	 * \param value
		//	 * \return True on success
		//	 */
		//	bool insert(const Key& key, const Value& value)
		//	{
		//		MapPtr p = std::atomic_load(&mapPtr_);
		//		MapPtr copy;
		//		do
		//		{
		//			if ((*p).count(key) > 0) return false;
		//			copy = std::make_shared<MapType>(*p);
		//			(*copy).insert({ key, value });
		//		} while (!std::atomic_compare_exchange_weak(&mapPtr_, &p, std::move(copy)));
		//		return true;
		//	}

		//	/*!
		//	 * \brief Erase according to key
		//	 * \param key
		//	 * \return True on success
		//	 */
		//	bool erase(const Key& key)
		//	{
		//		MapPtr p = std::atomic_load(&mapPtr_);
		//		MapPtr copy;
		//		do
		//		{
		//			if ((*p).count(key) <= 0) return false;
		//			copy = std::make_shared<MapType>(*p);
		//			(*copy).erase(key);
		//		} while (!std::atomic_compare_exchange_weak(&mapPtr_, &p, std::move(copy)));
		//		return true;
		//	}

		//	/*!
		//	 * \brief Clear all
		//	 */
		//	void clear()
		//	{
		//		MapPtr p = atomic_load(&mapPtr_);
		//		auto copy = std::make_shared<MapType>();
		//		do
		//		{
		//			;	// do clear when possible does not require old status
		//		} while (!std::atomic_compare_exchange_weak(&mapPtr_, &p, std::move(copy)));
		//	}

		//private:
		//	std::shared_ptr<MapType>	mapPtr_;
		//};

		///*!
		// * \brief AtomicNonTrivial Template lock-free class
		// * AtomicNonTrivial is lock-free, however, modification will create copies.
		// * Thus this structure is good for read-many write-rare purposes.
		// */
		//template <typename T> class AtomicNonTrivial
		//{
		//public:
		//	AtomicNonTrivial()
		//	{
		//		ptr_ = std::make_shared<T>();
		//	}

		//	/*!
		//	 * \brief Get shared_ptr to instance
		//	 * \return Shared_ptr to instance
		//	 */
		//	std::shared_ptr<T> get()
		//	{
		//		return std::atomic_load(&ptr_);
		//	}

		//	/*!
		//	 * \brief Set to new value
		//	 * \param val
		//	 * This operation will make a copy which is only visible for future get()
		//	 */
		//	void set(const T& val)
		//	{
		//		std::shared_ptr<T> copy = std::make_shared<T>(val);
		//		std::atomic_store(&ptr_, copy);
		//	}

		//private:
		//	std::shared_ptr<T>	ptr_;
		//};


		//		namespace gc
		//		{
		//			class HPRecord
		//			{
		//			public:
		//				void*	pHazard_;	//!< can be used by the thread that acquire it
		//				HPRecord*			pNext_;
		//				static HPRecord* head() { return pHead_; }
		//
		//				static HPRecord* acquire()
		//				{
		//					// try to reuse a retired hp record
		//					HPRecord* p = pHead_;
		//					bool inactive(false);
		//
		//					for (; p; p = p->pNext_)
		//					{
		//						if (p->active_ || (!p->active_.compare_exchange_weak(inactive, true)))
		//						{
		//							continue;
		//						}
		//						return p; // got one!
		//					}
		//					// increment the list length by adding one
		//					int oldLen;
		//					do
		//					{
		//						oldLen = listLen_;
		//					} while (!listLen_.compare_exchange_weak(oldLen, oldLen + 1));
		//					// allocate a new one
		//					p = new HPRecord;
		//					p->active_.store(true);
		//					p->pHazard_ = nullptr;
		//					// push it to the front
		//					HPRecord* old = nullptr;
		//					do
		//					{
		//						old = pHead_;
		//						p->pNext_ = old;
		//					} while (!pHead_.compare_exchange_weak(old, p));
		//
		//					return p; // return acquired pointer
		//				}
		//
		//				static void release(HPRecord* p)
		//				{
		//					p->pHazard_ = nullptr;
		//					p->active_.store(false);
		//				}
		//
		//			private:
		//				std::atomic<bool>	active_;
		//				static std::atomic<HPRecord*>	pHead_;	//!< global header of the hazard pointer list
		//				static std::atomic<int>			listLen_; //!< length of the list
		//			};
		//
		//			template <typename T> void garbage_collection(std::vector<T*> rlist)
		//			{
		//				// stage 1: scan hazard ponter list
		//				// collecting all non-null pointers
		//				std::vector<void*> hp;
		//				HPRecord* head = HPRecord::head();
		//				while (head)
		//				{
		//					void *p = head->pHazard_;
		//					if (p) hp.push_back(p);
		//					head = head->pNext_;
		//				}
		//				// stage 2: sort the hazard pointers
		//				std::sort(hp.begin(), hp.end(), std::less<void*>());
		//				// stage 3: search for null
		//				std::vector<T*>::iterator iter = rlist.begin();
		//				while (iter != rlist.end())
		//				{
		//					if (!std::binary_search(hp.begin(), hp.end(), *iter))
		//					{
		//						delete *iter;	// safely reclaim this memory
		//						if (&*iter != rlist.back())
		//						{
		//							*iter = rlist.back();
		//						}
		//						rlist.pop_back();
		//					}
		//					else
		//					{
		//						++iter;
		//					}
		//				}
		//			}
		//
		//		} // namespace gc
		//
		//		namespace consts
		//		{
		//			static const unsigned kGarbageCollectionThreshold = 4;
		//		}
		//
		//		template <typename K, typename V> class WRRMUMap
		//		{
		//		public:
		//
		//		private:
		//			using MapType = std::unordered_map<K, V>;
		//
		//			static void retire(MapType* old)
		//			{
		//				// put old map into retire list
		//				rlist_.push_back(old);
		//				if (rlist_.size() >= consts::kGarbageCollectionThreshold)
		//				{
		//					gc::garbage_collection(rlist_);
		//				}
		//			}
		//#if _MSC_VER
		//			static __declspec(thread)  std::vector<MapType*> rlist_;
		//#else
		//			static thread_local std::vector<MapType*> rlist_;
		//#endif
		//			MapType* pMap_;
		//		};


	} // namespace cds


	/*!
	 * \namespace zz::os
	 * \brief Namespace for OS specific implementations
	 */
	namespace os
	{
		/*!
		 * \fn int system(const char *const command, const char *const module_name = 0)
		 * \brief Execute sub-process using system call
		 * \param command
		 * \param module_name
		 * \return Return code of sub-process
		 */
		int system(const char *const command, const char *const module_name = 0);

		/*!
		 * \fn std::size_t thread_id()
		 * \brief Get thread id
		 * \return Current thread id
		 */
		std::size_t thread_id();

		/*!
		 * \brief Check if stdout is associated with console
		 * \return None zero value if stdout is not redirected to file
		 */
		int is_atty();

		/*!
		 * \fn std::tm localtime(std::time_t t)
		 * \brief Thread-safe version of localtime
		 * \param t std::time_t
		 * \return std::tm format localtime
		 */
		std::tm localtime(std::time_t t);

		/*!
		 * \fn std::tm gmtime(std::time_t t)
		 * \brief Thread-safe version of gmtime
		 * \param t std::time_t
		 * \return std::tm format UTC time
		 */
		std::tm gmtime(std::time_t t);

		/*!
		 * \brief Convert UTF-8 string to wstring
		 * \param u8str
		 * \return Converted wstring
		 */
		std::wstring utf8_to_wstring(std::string &u8str);

		/*!
		 * \brief Convert wstring to UTF-8
		 * \param wstr
		 * \return Converted UTF-8 string
		 */
		std::string wstring_to_utf8(std::wstring &wstr);

		/*!
		 * \brief Check if path exist in filesystem
		 * \param path
		 * \param considerFile Consider file as well?
		 * \return true if path exists
		 */
		bool path_exists(std::string path, bool considerFile = true);

		/*!
		 * \brief Check if path is file and exists
		 * \param path
		 * \return true if file exists, not directory
		 */
		bool is_file(std::string path);

		/*!
		 * \brief Check if path is directory and exists
		 * \param path
		 * \return true if directory exists, not file
		 */
		bool is_directory(std::string path);

		/*!
		 * \brief Open fstream using UTF-8 string
		 * This is a wrapper function because Windows will require wstring to process unicode filename/path.
		 * \param stream
		 * \param filename
		 * \param openmode
		 */
		void fstream_open(std::fstream &stream, std::string filename, std::ios::openmode openmode);

		/*!
		 * \brief Open ifstream using UTF-8 string
		 * This is a wrapper function because Windows will require wstring to process unicode filename/path.
		 * \param stream
		 * \param filename
		 * \param openmode
		 */
		void ifstream_open(std::ifstream &stream, std::string filename, std::ios::openmode openmode);

		/*!
		 * \brief Rename file, support unicode filename/path.
		 * \param oldName
		 * \param newName
		 * \return true on success
		 */
		bool rename(std::string oldName, std::string newName);

		/*!
		 * \brief Copy file, support unicode filename/path.
		 * \param oldName
		 * \param newName
		 * \param replaceDst If true, existing dst file will be replaced
		 * \return true on success
		 */
		bool copyfile(std::string src, std::string dst, bool replaceDst = false);

		/*!
		 * \brief Move file, support unicode filename/path.
		 * \param oldName
		 * \param newName
		 * \param replaceDst If true, existing dst file will be replaced
		 * \return true on success
		 */
		bool movefile(std::string src, std::string dst, bool replaceDst = false);

		/*!
		 * \brief Remove path, whatever file or directory.
		 * Dangerous! Cannot revert.
		 * \param path
		 * \return true as long as path does not exists anymore
		 */
		bool remove_all(std::string path);

		/*!
		 * \brief Remove directory and all sub-directories and files if set recursive to true.
		 * If recursive set to false, will try to delete an empty one only. Dangerous! Cannot revert.
		 * \param root
		 * \param recursive Delete all contents inside of it.
		 * \return true as long as directory does not exists anymore(file with same name may exist)
		 */
		bool remove_dir(std::string root, bool recursive = true);

		/*!
		 * \brief Remove file.
		 * Dangerous! Cannot revert.
		 * \param path
		 * \return true as long as file does not exists anymore
		 */
		bool remove_file(std::string path);

		/*!
		 * \brief Retrieve the last error in errno
		 * \return Human readable error string
		 */
		std::string last_error();

		/*!
		 * \brief	Gets the OS dependent line end characters.
		 * \return	A std::string.
		 */
		std::string endl();

		/*!
		 * \brief	Gets the OS dependent path delim.
		 * \return	A std::string.
		 */
		std::string path_delim();

		/*!
		 * \brief Get current working directory
		 * \return Current working directory
		 */
		std::string current_working_directory();

		/*!
		 * \brief Convert reletive path to absolute path
		 * \param reletivePath
		 * \return Absolute path
		 */
		std::string absolute_path(std::string reletivePath);

		/*!
		 * \brief Split path into hierachical sub-folders
		 * \param path
		 * \return std::vector<std::string> of sub-folders
		 * path_split("/usr/local/bin/xxx/")= {"usr","local","bin","xxx"}
		 */
		std::vector<std::string> path_split(std::string path);

		/*!
		 * \brief Compare identical path according to OS.
		 * By default, windows paths are case-INsensitive.
		 * \param first
		 * \param second
		 * \param forceCaseSensitve Force compare using case sensitivity.
		 * \return True if paths are indentical
		 */
		bool path_identical(std::string first, std::string second, bool forceCaseSensitve = false);

		/*!
		 * \brief Join path from sub-folders
		 * This function will handle system dependent path formats
		 * such as '/' for unix like OS, and '\\' for windows
		 * \param elems
		 * \return Long path
		 * path_join({"/home/abc/","def"} = "/home/abc/def"
		 */
		std::string path_join(std::vector<std::string> elems);

		/*!
		 * \brief Split filename from path
		 * \param path
		 * \return Full filename with extension
		 */
		std::string path_split_filename(std::string path);

		/*!
		 * \brief Split the deepest directory
		 * \param path
		 * \return Deepest directory name. E.g. "abc/def/ghi/xxx.xx"->"ghi"
		 */
		std::string path_split_directory(std::string path);

		/*!
		 * \brief Split basename
		 * \param path
		 * \return Basename without extension
		 */
		std::string path_split_basename(std::string path);

		/*!
		 * \brief Split extension if any
		 * \param path
		 * \return Extension or "" if no extension exists
		 */
		std::string path_split_extension(std::string path);

		/*!
		 * \brief Append string to basename directly, rather than append to extension
		 * This is more practically useful because nobody want to change extension in most situations.
		 * Appending string to basename in order to change filename happens.
		 * \param origPath
		 * \param whatToAppend
		 * \return New filename with appended string.
		 * path_append_basename("/home/test/abc.jpg", "_01") = "/home/test/abc_01.jpg"
		 */
		std::string path_append_basename(std::string origPath, std::string whatToAppend);

		/*!
		 * \brief Create directory if not exist
		 * \param path
		 * \return True if directory created/already exist
		 * \note Will not create recursively, fail if parent directory inexist. Use create_directory_recursive instead.
		 */
		bool create_directory(std::string path);

		/*!
		 * \brief Create directory recursively if not exist
		 * \param path
		 * \return True if directory created/already exist, false when failed to create the final directory
		 * Function create_directory_recursive will create directory recursively.
		 * For example, create_directory_recursive("/a/b/c") will create /a->/a/b->/a/b/c recursively.
		 */
		bool create_directory_recursive(std::string path);

		/*!
		 * \brief List directory contents
		 * \param root Root of the directory
		 * \return A std::vecotr<std::string>, vector of absolute paths of files and sub-directories
		 */
		std::vector<std::string> list_directory(std::string root);
	} // namespace os

	/*!
	 * \namespace zz::time
	 * \brief Namespace for time related stuff
	 */
	namespace time
	{
		/*!
		* \class	DateTime
		*
		* \brief	A calendar date class.
		*/
		class DateTime
		{
		public:
			DateTime();
			virtual ~DateTime() = default;

			/*!
			 * \brief Convert to local time zone
			 */
			void to_local_time();

			/*!
			 * \brief Convert to UTC time zone
			 */
			void to_utc_time();

			/*!
			 * \brief Convert date to user friendly string.
			 * Support various formats.
			 * \param format
			 * \return Readable string
			 */
			std::string to_string(const char *format = "%y-%m-%d %H:%M:%S.%frac");

			/*!
			 * \brief Static function to return in local_time
			 * \return DateTime instance
			 */
			static DateTime local_time();

			/*!
			 * \brief Static function to return in utc_time
			 * \return DateTime instance
			 */
			static DateTime utc_time();

		private:
			std::time_t		timeStamp_;
			int				fraction_;
			std::string		fractionStr_;
			struct std::tm	calendar_;

		};

		/*!
		* \class	Timer
		*
		* \brief	A timer class.
		*/
		class Timer
		{
		public:
			Timer();
			/*!
			 * \brief Reset timer to record new process
			 */
			void reset();

			/*!
			* \brief Pause recording timelapse
			*/
			void pause();

			/*!
			* \brief Resume timer
			*/
			void resume();

			/*!
			 * \brief Return elapsed time quantized in nanosecond
			 * \return Nanosecond elapsed
			 */
			std::size_t	elapsed_ns();

			/*!
			 * \brief Return string of elapsed time quantized in nanosecond
			 * \return Nanosecond elapsed in string
			 */
			std::string elapsed_ns_str();

			/*!
			 * \brief Return elapsed time quantized in microsecond
			 * \return Microsecond elapsed
			 */
			std::size_t elapsed_us();

			/*!
			 * \brief Return string of elapsed time quantized in microsecond
			 * \return Microsecond elapsed in string
			 */
			std::string elapsed_us_str();

			/*!
			 * \brief Return elapsed time quantized in millisecond
			 * \return Millisecond elapsed
			 */
			std::size_t elapsed_ms();

			/*!
			 * \brief Return string of elapsed time quantized in millisecond
			 * \return Millisecond elapsed in string
			 */
			std::string elapsed_ms_str();

			/*!
			 * \brief Return elapsed time quantized in second
			 * \return Second elapsed
			 */
			std::size_t elapsed_sec();

			/*!
			 * \brief Return string of elapsed time quantized in second
			 * \return Second elapsed in string
			 */
			std::string elapsed_sec_str();

			/*!
			 * \brief Return elapsed time in second, no quantization
			 * \return Second elapsed in double
			 */
			double elapsed_sec_double();

			/*!
			 * \brief Convert timer to user friendly string.
			 * Support various formats
			 * \param format
			 * \return Formatted string
			 */
			std::string to_string(const char *format = "[%ms ms]");

		private:
			std::chrono::steady_clock::time_point timeStamp_;
			std::size_t	elapsed_;
			bool		paused_;
		};

		/*!
		* \fn	void sleep(int milliseconds);
		*
		* \brief	Sleep for specified milliseconds
		*
		* \param	milliseconds	The milliseconds.
		*/
		inline void sleep(int milliseconds)
		{
			if (milliseconds > 0)
			{
				std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
			}
		}

	} // namespace time

	/*!
	 * \namespace zz::fs
	 * \brief Namespace for classes adpated to filesystems
	 */
	namespace fs
	{
		namespace consts
		{
			static const int kDefaultFileOpenRetryTimes = 5;
			static const int kDefaultFileOpenRetryInterval = 10;
		}

		/*!
		* \brief The Path class to resolve filesystem path
		*/
		class Path
		{
		public:
			Path(std::string path, bool isAbsolute = false);

			/*!
			 * \brief Check if path is empty
			 * \return True if empty
			 */
			bool empty() const;

			/*!
			 * \brief Check path existance, whatever file/dir/device...
			 * \return True if path exist
			 */
			bool exist() const;

			/*!
			 * \brief Check if path is a file and exist
			 * \return True if file exist
			 */
			bool is_file() const;

			/*!
			 * \brief Check if path is a directory and exist
			 * \return True if dir exist
			 */
			bool is_dir() const;

			/*!
			 * \brief Return Absolute path
			 * \return A std::string of absolute path
			 */
			std::string abs_path() const;

			/*!
			 * \brief Return relative path to current working directory
			 * \return A std::string of relative path to cwd
			 */
			std::string relative_path() const;

			/*!
			 * \brief Return relative path to specified root.
			 * \param root Specified root path
			 * \return A std::string of relative path to root
			 */
			std::string relative_path(std::string root) const;

			/*!
			 * \brief Return filename if is file and exist.
			 * Will return empty string if path is not a file
			 * \return A std::string of filename
			 */
			std::string filename() const;

		private:
			std::string abspath_;
		};

		/*!
		* \brief The Directory class for filesystem directory operations
		*/
		class Directory
		{
		public:
			using iterator = std::vector<Path>::iterator;
			using const_iterator = std::vector<Path>::const_iterator;

			/*!
			 * \brief Directory constructor
			 * \param root Root of directory
			 * \param recursive Recursive search or not
			 */
			Directory(std::string root, bool recursive = false);

			/*!
			 * \brief Directory constructor with filter pattern
			 * \param root Root of directory
			 * \param pattern Filter pattern
			 * \param recursive Recursive search or not
			 */
			Directory(std::string root, const std::string pattern, bool recursive = false);

			/*!
			 * \brief Directory constructor with filter pattern
			 * \param root Root of directory
			 * \param patternList Vector of filter patterns
			 * \param recursive Recursive search or not
			 */
			Directory(std::string root, const std::vector<std::string> patternList, bool recursive);

			/*!
			 * \brief Directory constructor with filter pattern
			 * \param root Root of directory
			 * \param patternList Vector of filter patterns
			 * \param recursive Recursive search or not
			 */
			Directory(std::string root, const std::vector<const char*> patternList, bool recursive);

			/*!
			 * \brief Return begin iterator
			 * \return Iterator to begin
			 */
			iterator begin() { return paths_.begin(); }

			/*!
			 * \brief Return end iterator
			 * \return Iterator to end
			 */
			iterator end() { return paths_.end(); }

			/*!
			 * \brief Return const begin iterator
			 * \return Iterator to cbegin
			 */
			const_iterator cbegin() const { return paths_.cbegin(); }

			/*!
			 * \brief Return const end iterator
			 * \return Iterator to cend
			 */
			const_iterator cend() const { return paths_.cend(); }

			/*!
			 * \brief Return number of contained directories or files
			 * \return Size
			 */
			std::size_t size() const { return paths_.size(); }

			/*!
			* \brief Check if directory is resursively searched
			* \return True if recursive
			*/
			bool is_recursive() const;

			/*!
			 * \brief Return root of directory
			 * \return A std::string of absolute root path
			 */
			std::string root() const;

			/*!
			 * \brief Filter directory with specified pattern
			 * \param Wild card matching pattern
			 */
			void filter(const std::string pattern);

			/*!
			 * \brief Filter directory with multiple patterns
			 * \param patternList Vector of wild card matching patterns
			 */
			void filter(const std::vector<const char*> patternList);

			/*!
			 * \brief Filter directory with multiple patterns
			 * \param patternList Vector of wild card matching patterns
			 */
			void filter(const std::vector<std::string> patternList);

			/*!
			 * \brief Clear filter pattern, redo search in directory
			 */
			void reset();

			/*!
			 * \brief Return the entire list
			 * \return A std::vector<Path> containing current list
			 */
			std::vector<Path> to_list() const;

		private:
			void resolve();
  
      Path root_;
			bool recursive_;
			std::vector<Path> paths_;
		};

		/*!
		 * \brief The FileEditor class to modify file
		 * This class is derived from UnCopyable, so no copy operation.
		 * Move operation is allowed by std::move();
		 */
		class FileEditor : private UnCopyable
		{
		public:
			FileEditor() = default;

			/*!
			 * \brief FileEditor constructor
			 * \param filename
			 * \param truncateOrNot Whether open in truncate mode or not
			 * \param retryTimes Retry open file if not success
			 * \param retryInterval Retry interval in ms
			 */
			FileEditor(std::string filename, bool truncateOrNot = false,
				int retryTimes = consts::kDefaultFileOpenRetryTimes,
				int retryInterval = consts::kDefaultFileOpenRetryInterval);

			/*!
			* \fn	File::File(File&& from)
			*
			* \brief	Move constructor.
			*
			* \param [in,out]	other	Source for the instance.
			*/
			//FileEditor(FileEditor&& other);

			virtual ~FileEditor() { this->close(); }

			// No move operator
			FileEditor& operator=(FileEditor&&) = delete;

			/*!
			 * \brief Overload << operator just like a stream
			 */
			template <typename T>
			FileEditor& operator<<(T what) { stream_ << what; return *this; }

			/*!
			 * \brief Return filename
			 * \return Filename of this object
			 */
			std::string filename() const { return filename_; }

			/*!
			 * \brief Open file
			 * \param filename
			 * \param truncateOrNot Whether open in truncate mode or not
			 * \param retryTimes Retry open file if not success
			 * \param retryInterval Retry interval in ms
			 * \return True if success
			 */
			bool open(std::string filename, bool truncateOrNot = false,
				int retryTimes = consts::kDefaultFileOpenRetryTimes,
				int retryInterval = consts::kDefaultFileOpenRetryInterval);

			/*!
			 * \brief Open file
			 * \param filename
			 * \param truncateOrNot Whether open in truncate mode or not
			 * \param retryTimes Retry open file if not success
			 * \param retryInterval Retry interval in ms
			 * \return True if success
			 */
			bool open(const char* filename, bool truncateOrNot = false,
				int retryTimes = consts::kDefaultFileOpenRetryTimes,
				int retryInterval = consts::kDefaultFileOpenRetryInterval);

			/*!
			 * \brief Open file
			 * \param truncateOrNot Whether open in truncate mode or not
			 * \return True if success
			 */
			bool open(bool truncateOrNot = false);

			/*!
			 * \brief Reopen current file
			 * \param truncateOrNot
			 * \return True if success
			 */
			bool reopen(bool truncateOrNot = true);

			/*!
			 * \brief Close current file
			 */
			void close();

			/*!
			 * \brief Check whether current file handler is set
			 * \return True if valid file
			 */
			bool is_valid() const { return !filename_.empty(); }

			/*!
			 * \brief Check if file is opened
			 * \return True if opened
			 */
			bool is_open() const { return stream_.is_open(); }

			/*!
			 * \brief Flush file stream
			 */
			void flush() { stream_.flush(); }
		private:

			bool try_open(int retryTime, int retryInterval, bool truncateOrNot = false);
			void check_valid() { if (!this->is_valid()) throw RuntimeException("Invalid File Editor!"); }

			std::string		filename_;
			std::fstream	stream_;
			std::streampos	readPos_;
			std::streampos	writePos_;
		};

		/*!
		 * \brief The FileReader class for read-only operations.
		 * This class is derived from UnCopyable, so no copy operation.
		 * Move operation is allowed by std::move();
		 */
		class FileReader : private UnCopyable
		{
		public:
			FileReader() = delete;

			/*!
			 * \brief FileReader constructor
			 * \param filename
			 * \param retryTimes Retry open times
			 * \param retryInterval Retry interval in ms
			 */
			FileReader(std::string filename, int retryTimes = consts::kDefaultFileOpenRetryTimes,
				int retryInterval = consts::kDefaultFileOpenRetryInterval);

			/*!
			 * \brief FileReader move constructor
			 * \param other
			 */
			//FileReader(FileReader&& other);

			/*!
			 * \brief Return filename
			 * \return Filename
			 */
			std::string filename() const { return filename_; }

			/*!
			 * \brief Check if is opened
			 * \return True if opened
			 */
			bool is_open() const { return istream_.is_open(); }

			/*!
			 * \brief Check if valid filename is set
			 * \return True if valid
			 */
			bool is_valid() const { return !filename_.empty(); }

			/*!
			 * \brief Close file handler
			 */
			void close() { istream_.close(); }

			/*!
			 * \brief Get file size in byte, member function
			 * \return File size in byte
			 */
			std::size_t	file_size();

			/*!
			 * \brief Count number of lines in text file
			 * \return Number of lines
			 */
			std::size_t count_lines();

			/*!
			 * \brief Get next line
			 * \param trimWhitespaces Whether or not trim whitespaces
			 * \return A std::string of the line
			 */
			std::string next_line(bool trimWhiteSpaces = false);

			/*!
			 * \brief Count number of lines in text file
			 * If reached end of file, will return the number of lines
			 * \param n Line # to jump to
			 * \return -1 if failed, otherwise return the position actually jumped to
			 */
			int goto_line(int n);

		private:
			bool open();
			bool try_open(int retryTime, int retryInterval);
			void check_valid(){ if (!this->is_valid()) throw RuntimeException("Invalid File Reader!"); }
			std::string		filename_;
			std::ifstream	istream_;
		};

		/*!
		 * \brief Get file size in byte
		 * \param filename
		 * \return File size in byte, 0 if empty or any error occurred.
		 */
		std::size_t get_file_size(std::string filename);

	} //namespace fs


	/*!
	 * \namespace zz::fmt
	 * \brief Namespace for formatting functions
	 */
	namespace fmt
	{
		namespace consts
		{
			static const std::string kFormatSpecifierPlaceHolder = std::string("{}");
		}

		namespace detail
		{
			template<class Facet>
			struct DeletableFacet : Facet
			{
				template<class ...Args>
				DeletableFacet(Args&& ...args) : Facet(std::forward<Args>(args)...) {}
				~DeletableFacet() {}
			};
		} // namespace detail

		/*!
		 * \brief Convert int to left zero padded string
		 * \param num Integer number
		 * \param length String length
		 * \return Zero-padded string
		 */
		inline std::string int_to_zero_pad_str(int num, int length)
		{
			std::ostringstream ss;
			ss << std::setw(length) << std::setfill('0') << num;
			return ss.str();
		}

		/*!
		 * \brief Check if is digit
		 * \param c
		 * \return
		 */
		bool is_digit(char c);

		/*!
		 * \brief Match string with wildcard.
		 * Match string with wildcard pattern, '*' and '?' supported.
		 * \param str
		 * \param pattern
		 * \return True for success match
		 */
		bool wild_card_match(const char* str, const char* pattern);

		/*!
		 * \brief Check if string starts with specified sub-string
		 * \param str
		 * \param start
		 * \return True if success
		 */
		bool starts_with(const std::string& str, const std::string& start);

		/*!
		 * \brief Check if string ends with specified sub-string
		 * \param str
		 * \param end
		 * \return True if success
		 */
		bool ends_with(const std::string& str, const std::string& end);

		/*!
		 * \brief Replace all from one to another sub-string
		 * \param str
		 * \param replaceWhat
		 * \param replaceWith
		 * \return Reference to modified string
		 */
		std::string& replace_all(std::string& str, const std::string& replaceWhat, const std::string& replaceWith);

		/*!
		 * \brief Replace all from one to another sub-string, char version
		 * \param str
		 * \param replaceWhat
		 * \param replaceWith
		 * \return Reference to modified string
		 */
		std::string& replace_all(std::string& str, char replaceWhat, char replaceWith);

		/*!
		 * \brief Compare c style raw string
		 * Will take care of string not ends with '\0'(ignore it),
		 * which is unsafe using strcmp().
		 * \param s1
		 * \param s2
		 * \return True if same
		 */
		bool str_equals(const char* s1, const char* s2);

		/*!
		 * \brief Left trim whitespace
		 * \param str
		 * \return Trimed string
		 */
		std::string ltrim(std::string str);

		/*!
		 * \brief Right trim whitespace
		 * \param str
		 * \return Trimed string
		 */
		std::string rtrim(std::string str);

		/*!
		 * \brief Left and right trim whitespace
		 * \param str
		 * \return Trimed string
		 */
		std::string trim(std::string str);

		/*!
		 * \brief Strip specified sub-string from left.
		 * The strip will do strict check from left, even whitespace.
		 * \param str Original string
		 * \param what What to be stripped
		 * \return Stripped string
		 */
		std::string lstrip(std::string str, std::string what);

		/*!
		 * \brief Strip specified sub-string from right.
		 * The strip will do strict check from right, even whitespace.
		 * \param str Original string
		 * \param what What to be stripped
		 * \return Stripped string
		 */
		std::string rstrip(std::string str, std::string what);

		/*!
		* \brief Skip from left until delimiter string found.
		* \param str
		* \param delim
		* \return Skipped string
		*/
		std::string lskip(std::string str, std::string delim);

		/*!
		 * \brief Skip from right until delimiter string found.
		 * \param str
		 * \param delim
		 * \return Skipped string
		 */
		std::string rskip(std::string str, std::string delim);

		/*!
		 * \brief Skip from right, remove all stuff right to left-most delim
		 * \param str
		 * \param delim
		 * \return Skipped string
		 */
		std::string rskip_all(std::string str, std::string delim);

		/*!
		 * \brief Split string into parts with specified single char delimiter
		 * \param s
		 * \param delim
		 * \return A std::vector of parts in std::string
		 */
		std::vector<std::string> split(const std::string s, char delim = ' ');

		/*!
		 * \brief Split string into parts with specified string delimiter.
		 * The entire delimiter must be matched exactly
		 * \param s
		 * \param delim
		 * \return A std::vector of parts in std::string
		 */
		std::vector<std::string> split(const std::string s, std::string delim);

		/*!
		 * \brief Split string into parts with multiple single char delimiter
		 * \param s
		 * \param delim A std::string contains multiple single char delimiter, e.g.(' \t\n')
		 * \return A std::vector of parts in std::string
		 */
		std::vector<std::string> split_multi_delims(const std::string s, std::string delims);

		/*!
		 * \brief Special case to split_multi_delims(), split all whitespace.
		 * \param s
		 * \return A std::vector of parts in std::string
		 */
		std::vector<std::string> split_whitespace(const std::string s);

		/*!
		 * \brief Split string in two parts with specified delim
		 * \param s
		 * \param delim
		 * \return A std::pair of std::string, ret.first = first part, ret.second = second part
		 */
		std::pair<std::string, std::string> split_first_occurance(const std::string s, char delim);

		/*!
		 * \brief Concatenates a std::vector of strings into a string with delimiters
		 * \param elems
		 * \param delim
		 * \return Concatenated string
		 */
		std::string join(std::vector<std::string> elems, char delim);

		/*!
		 * \brief Go through vector and erase empty ones.
		 * Erase in-place in vector.
		 * \param vec
		 * \return Clean vector with no empty elements.
		 */
		std::vector<std::string>& erase_empty(std::vector<std::string>& vec);

		/*!
		 * \brief Replace first occurance of one string with specified another string.
		 * Replace in-place.
		 * \param str
		 * \param replaceWhat What substring to be replaced.
		 * \param replaceWith What string to replace.
		 */
		void replace_first_with_escape(std::string &str, const std::string &replaceWhat, const std::string &replaceWith);

		/*!
		 * \brief Replace every occurance of one string with specified another string.
		 * Replace in-place.
		 * \param str
		 * \param replaceWhat What substring to be replaced.
		 * \param replaceWith What string to replace.
		 */
		void replace_all_with_escape(std::string &str, const std::string &replaceWhat, const std::string &replaceWith);

		/*!
		* \brief Replace every occurance of one string with specified list of strings sequentially.
		* Replace in-place.
		* \param str
		* \param replaceWhat What substring to be replaced.
		* \param replaceWith Vector of what strings to replace one by one.
		*/
		void replace_sequential_with_escape(std::string &str, const std::string &replaceWhat, const std::vector<std::string> &replaceWith);

		/*!
		 * \brief Convert string to lower case.
		 * Support ASCII characters only. Unicode string will trigger undefined behavior.
		 * \param mixed Mixed case string
		 * \return Lower case string.
		 */
		std::string to_lower_ascii(std::string mixed);

		/*!
		 * \brief Convert string to upper case.
		 * Support ASCII characters only. Unicode string will trigger undefined behavior.
		 * \param mixed Mixed case string
		 * \return Upper case string.
		 */
		std::string to_upper_ascii(std::string mixed);

		/*!
		 * \brief C++ 11 UTF-8 string to UTF-16 string
		 * \param u8str UTF-8 string
		 * \return UTF-16 string
		 */
		std::u16string utf8_to_utf16(std::string u8str);

		/*!
		 * \brief C++ 11 UTF-16 string to UTF-8 string
		 * \param u16str UTF-16 string
		 * \return UTF-8 string
		 */
		std::string utf16_to_utf8(std::u16string u16str);

		/*!
		 * \brief C++ 11 UTF-8 string to UTF-32 string
		 * \param u8str UTF-8 string
		 * \return UTF-32 string
		 */
		std::u32string utf8_to_utf32(std::string u8str);

		/*!
		 * \brief C++ 11 UTF-32 string to UTF-8 string
		 * \param u32str UTF-32 string
		 * \return UTF-8 string
		 */
		std::string utf32_to_utf8(std::u32string u32str);

		/*!
		 * \fn template<typename Arg> inline void format_string(std::string &fmt, const Arg &last)
		 * \brief Format function to replace each {} with templated type variable
		 * \param fmt Original string with place-holder {}
		 * \param last The last variable in variadic template function
		 */
		template<typename Arg>
		inline void format_string(std::string &fmt, const Arg &last)
		{
			std::stringstream ss;
			ss << last;
			replace_first_with_escape(fmt, consts::kFormatSpecifierPlaceHolder, ss.str());
		}

		/*!
		 * \fn template<typename Arg> inline void format_string(std::string &fmt, const Arg& current, const Args&... more)
		 * \brief Vairadic variable version of format function to replace each {} with templated type variable
		 * \param fmt Original string with place-holder {}
		 * \param current The current variable to be converted
		 * \param ... more Variadic variables to be templated.
		 */
		template<typename Arg, typename... Args>
		inline void format_string(std::string &fmt, const Arg& current, const Args&... more)
		{
			std::stringstream ss;
			ss << current;
			replace_first_with_escape(fmt, consts::kFormatSpecifierPlaceHolder, ss.str());
			format_string(fmt, more...);
		}

	} // namespace fmt

} // namespace zz

#endif //END _ZUPPLY_ZUPPLY_HPP_
