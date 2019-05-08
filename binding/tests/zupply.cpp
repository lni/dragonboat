/********************************************************************//*
 *
 *   Script File: zupply.cpp
 *
 *   Description:
 *
 *   Implementations of core modules
 *
 *
 *   Author: Joshua Zhang
 *   DateTime since: June-2015
 *
 *   Copyright (c) <2015> <JOSHUA Z. ZHANG>
 *
 *	 Open source according to MIT License.
 *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 *   IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 *   CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 *   TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 *   SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 ***************************************************************************/

// Unless you are very confident, don't set either OS flag
#if defined(ZUPPLY_OS_UNIX) && defined(ZUPPLY_OS_WINDOWS)
#error Both Unix and Windows flags are set, which is not allowed!
#elif defined(ZUPPLY_OS_UNIX)
#pragma message Using defined Unix flag
#elif defined(ZUPPLY_OS_WINDOWS)
#pragma message Using defined Windows flag
#else
#if defined(unix)        || defined(__unix)      || defined(__unix__) \
	|| defined(linux) || defined(__linux) || defined(__linux__) \
	|| defined(sun) || defined(__sun) \
	|| defined(BSD) || defined(__OpenBSD__) || defined(__NetBSD__) \
	|| defined(__FreeBSD__) || defined (__DragonFly__) \
	|| defined(sgi) || defined(__sgi) \
	|| (defined(__MACOSX__) || defined(__APPLE__)) \
	|| defined(__CYGWIN__) || defined(__MINGW32__)
#define ZUPPLY_OS_UNIX	1	//!< Unix like OS(POSIX compliant)
#undef ZUPPLY_OS_WINDOWS
#elif defined(_MSC_VER) || defined(WIN32)  || defined(_WIN32) || defined(__WIN32__) \
	|| defined(WIN64) || defined(_WIN64) || defined(__WIN64__)
#define ZUPPLY_OS_WINDOWS	1	//!< Microsoft Windows
#undef ZUPPLY_OS_UNIX
#else
#error Unable to support this unknown OS.
#endif
#endif

#if ZUPPLY_OS_WINDOWS
#if _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#define _CRT_NONSTDC_NO_DEPRECATE
#endif
#include <windows.h>
#include <direct.h>
#include <stdlib.h>
#include <stdio.h>
#include <conio.h>
#include <io.h>
#elif ZUPPLY_OS_UNIX
#include <unistd.h>	/* POSIX flags */
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <ftw.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#endif

#include "zupply.h"

#include <chrono>
#include <iomanip>
#include <ctime>
#include <sstream>
#include <iostream>
#include <string>
#include <vector>
#include <utility>
#include <algorithm>
#include <functional>
#include <cctype>
#include <cstdio>
#include <cmath>
#include <cstring>
#include <deque>
#include <cstdarg>

// UTF8CPP
#include <stdexcept>
#include <iterator>

namespace zz
{
	// \cond
	// This namespace scopes all thirdparty libraries
	namespace thirdparty
	{
		namespace utf8
		{
			// The typedefs for 8-bit, 16-bit and 32-bit unsigned integers
			// You may need to change them to match your system.
			// These typedefs have the same names as ones from cstdint, or boost/cstdint
			typedef unsigned char   uint8_t;
			typedef unsigned short  uint16_t;
			typedef unsigned int    uint32_t;

			// Helper code - not intended to be directly called by the library users. May be changed at any time
			namespace internal
			{
				// Unicode constants
				// Leading (high) surrogates: 0xd800 - 0xdbff
				// Trailing (low) surrogates: 0xdc00 - 0xdfff
				const uint16_t LEAD_SURROGATE_MIN = 0xd800u;
				//const uint16_t LEAD_SURROGATE_MAX = 0xdbffu;
				const uint16_t TRAIL_SURROGATE_MIN = 0xdc00u;
				const uint16_t TRAIL_SURROGATE_MAX = 0xdfffu;
				const uint16_t LEAD_OFFSET = LEAD_SURROGATE_MIN - (0x10000 >> 10);
				const uint32_t SURROGATE_OFFSET = 0x10000u - (LEAD_SURROGATE_MIN << 10) - TRAIL_SURROGATE_MIN;

				// Maximum valid value for a Unicode code point
				const uint32_t CODE_POINT_MAX = 0x0010ffffu;

				template<typename octet_type>
				inline uint8_t mask8(octet_type oc)
				{
					return static_cast<uint8_t>(0xff & oc);
				}
				template<typename u16_type>
				inline uint16_t mask16(u16_type oc)
				{
					return static_cast<uint16_t>(0xffff & oc);
				}
				template<typename octet_type>
				inline bool is_trail(octet_type oc)
				{
					return ((mask8(oc) >> 6) == 0x2);
				}

				template <typename u16>
				inline bool is_surrogate(u16 cp)
				{
					return (cp >= LEAD_SURROGATE_MIN && cp <= TRAIL_SURROGATE_MAX);
				}

				template <typename u32>
				inline bool is_code_point_valid(u32 cp)
				{
					return (cp <= CODE_POINT_MAX && !is_surrogate(cp) && cp != 0xfffe && cp != 0xffff);
				}

				template <typename octet_iterator>
				inline typename std::iterator_traits<octet_iterator>::difference_type
					sequence_length(octet_iterator lead_it)
				{
						uint8_t lead = mask8(*lead_it);
						if (lead < 0x80)
							return 1;
						else if ((lead >> 5) == 0x6)
							return 2;
						else if ((lead >> 4) == 0xe)
							return 3;
						else if ((lead >> 3) == 0x1e)
							return 4;
						else
							return 0;
					}

				enum utf_error { OK, NOT_ENOUGH_ROOM, INVALID_LEAD, INCOMPLETE_SEQUENCE, OVERLONG_SEQUENCE, INVALID_CODE_POINT };

				template <typename octet_iterator>
				utf_error validate_next(octet_iterator& it, octet_iterator end, uint32_t* code_point)
				{
					uint32_t cp = mask8(*it);
					// Check the lead octet
					typedef typename std::iterator_traits<octet_iterator>::difference_type octet_difference_type;
					octet_difference_type length = sequence_length(it);

					// "Shortcut" for ASCII characters
					if (length == 1) {
						if (end - it > 0) {
							if (code_point)
								*code_point = cp;
							++it;
							return OK;
						}
						else
							return NOT_ENOUGH_ROOM;
					}

					// Do we have enough memory?
					if (std::distance(it, end) < length)
						return NOT_ENOUGH_ROOM;

					// Check trail octets and calculate the code point
					switch (length) {
					case 0:
						return INVALID_LEAD;
						break;
					case 2:
						if (is_trail(*(++it))) {
							cp = ((cp << 6) & 0x7ff) + ((*it) & 0x3f);
						}
						else {
							--it;
							return INCOMPLETE_SEQUENCE;
						}
						break;
					case 3:
						if (is_trail(*(++it))) {
							cp = ((cp << 12) & 0xffff) + ((mask8(*it) << 6) & 0xfff);
							if (is_trail(*(++it))) {
								cp += (*it) & 0x3f;
							}
							else {
								std::advance(it, -2);
								return INCOMPLETE_SEQUENCE;
							}
						}
						else {
							--it;
							return INCOMPLETE_SEQUENCE;
						}
						break;
					case 4:
						if (is_trail(*(++it))) {
							cp = ((cp << 18) & 0x1fffff) + ((mask8(*it) << 12) & 0x3ffff);
							if (is_trail(*(++it))) {
								cp += (mask8(*it) << 6) & 0xfff;
								if (is_trail(*(++it))) {
									cp += (*it) & 0x3f;
								}
								else {
									std::advance(it, -3);
									return INCOMPLETE_SEQUENCE;
								}
							}
							else {
								std::advance(it, -2);
								return INCOMPLETE_SEQUENCE;
							}
						}
						else {
							--it;
							return INCOMPLETE_SEQUENCE;
						}
						break;
					}
					// Is the code point valid?
					if (!is_code_point_valid(cp)) {
						for (octet_difference_type i = 0; i < length - 1; ++i)
							--it;
						return INVALID_CODE_POINT;
					}

					if (code_point)
						*code_point = cp;

					if (cp < 0x80) {
						if (length != 1) {
							std::advance(it, -(length - 1));
							return OVERLONG_SEQUENCE;
						}
					}
					else if (cp < 0x800) {
						if (length != 2) {
							std::advance(it, -(length - 1));
							return OVERLONG_SEQUENCE;
						}
					}
					else if (cp < 0x10000) {
						if (length != 3) {
							std::advance(it, -(length - 1));
							return OVERLONG_SEQUENCE;
						}
					}

					++it;
					return OK;
				}

				template <typename octet_iterator>
				inline utf_error validate_next(octet_iterator& it, octet_iterator end) {
					return validate_next(it, end, 0);
				}

			} // namespace internal

			/// The library API - functions intended to be called by the users

			// Byte order mark
			const uint8_t bom[] = { 0xef, 0xbb, 0xbf };

			template <typename octet_iterator>
			octet_iterator find_invalid(octet_iterator start, octet_iterator end)
			{
				octet_iterator result = start;
				while (result != end) {
					internal::utf_error err_code = internal::validate_next(result, end);
					if (err_code != internal::OK)
						return result;
				}
				return result;
			}

			template <typename octet_iterator>
			inline bool is_valid(octet_iterator start, octet_iterator end)
			{
				return (find_invalid(start, end) == end);
			}

			template <typename octet_iterator>
			inline bool is_bom(octet_iterator it)
			{
				return (
					(internal::mask8(*it++)) == bom[0] &&
					(internal::mask8(*it++)) == bom[1] &&
					(internal::mask8(*it)) == bom[2]
					);
			}

			// Exceptions that may be thrown from the library functions.
			class invalid_code_point : public std::exception {
				uint32_t cp;
			public:
				invalid_code_point(uint32_t _cp) : cp(_cp) {}
				virtual const char* what() const throw() { return "Invalid code point"; }
				uint32_t code_point() const { return cp; }
			};

			class invalid_utf8 : public std::exception {
				uint8_t u8;
			public:
				invalid_utf8(uint8_t u) : u8(u) {}
				virtual const char* what() const throw() { return "Invalid UTF-8"; }
				uint8_t utf8_octet() const { return u8; }
			};

			class invalid_utf16 : public std::exception {
				uint16_t u16;
			public:
				invalid_utf16(uint16_t u) : u16(u) {}
				virtual const char* what() const throw() { return "Invalid UTF-16"; }
				uint16_t utf16_word() const { return u16; }
			};

			class not_enough_room : public std::exception {
			public:
				virtual const char* what() const throw() { return "Not enough space"; }
			};

			/// The library API - functions intended to be called by the users

			template <typename octet_iterator, typename output_iterator>
			output_iterator replace_invalid(octet_iterator start, octet_iterator end, output_iterator out, uint32_t replacement)
			{
				while (start != end) {
					octet_iterator sequence_start = start;
					internal::utf_error err_code = internal::validate_next(start, end);
					switch (err_code) {
					case internal::OK:
						for (octet_iterator it = sequence_start; it != start; ++it)
							*out++ = *it;
						break;
					case internal::NOT_ENOUGH_ROOM:
						throw not_enough_room();
					case internal::INVALID_LEAD:
						append(replacement, out);
						++start;
						break;
					case internal::INCOMPLETE_SEQUENCE:
					case internal::OVERLONG_SEQUENCE:
					case internal::INVALID_CODE_POINT:
						append(replacement, out);
						++start;
						// just one replacement mark for the sequence
						while (internal::is_trail(*start) && start != end)
							++start;
						break;
					}
				}
				return out;
			}

			template <typename octet_iterator, typename output_iterator>
			inline output_iterator replace_invalid(octet_iterator start, octet_iterator end, output_iterator out)
			{
				static const uint32_t replacement_marker = internal::mask16(0xfffd);
				return replace_invalid(start, end, out, replacement_marker);
			}

			template <typename octet_iterator>
			octet_iterator append(uint32_t cp, octet_iterator result)
			{
				if (!internal::is_code_point_valid(cp))
					throw invalid_code_point(cp);

				if (cp < 0x80)                        // one octet
					*(result++) = static_cast<uint8_t>(cp);
				else if (cp < 0x800) {                // two octets
					*(result++) = static_cast<uint8_t>((cp >> 6) | 0xc0);
					*(result++) = static_cast<uint8_t>((cp & 0x3f) | 0x80);
				}
				else if (cp < 0x10000) {              // three octets
					*(result++) = static_cast<uint8_t>((cp >> 12) | 0xe0);
					*(result++) = static_cast<uint8_t>(((cp >> 6) & 0x3f) | 0x80);
					*(result++) = static_cast<uint8_t>((cp & 0x3f) | 0x80);
				}
				else if (cp <= internal::CODE_POINT_MAX) {      // four octets
					*(result++) = static_cast<uint8_t>((cp >> 18) | 0xf0);
					*(result++) = static_cast<uint8_t>(((cp >> 12) & 0x3f) | 0x80);
					*(result++) = static_cast<uint8_t>(((cp >> 6) & 0x3f) | 0x80);
					*(result++) = static_cast<uint8_t>((cp & 0x3f) | 0x80);
				}
				else
					throw invalid_code_point(cp);

				return result;
			}

			template <typename octet_iterator>
			uint32_t next(octet_iterator& it, octet_iterator end)
			{
				uint32_t cp = 0;
				internal::utf_error err_code = internal::validate_next(it, end, &cp);
				switch (err_code) {
				case internal::OK:
					break;
				case internal::NOT_ENOUGH_ROOM:
					throw not_enough_room();
				case internal::INVALID_LEAD:
				case internal::INCOMPLETE_SEQUENCE:
				case internal::OVERLONG_SEQUENCE:
					throw invalid_utf8(*it);
				case internal::INVALID_CODE_POINT:
					throw invalid_code_point(cp);
				}
				return cp;
			}

			template <typename octet_iterator>
			uint32_t peek_next(octet_iterator it, octet_iterator end)
			{
				return next(it, end);
			}

			template <typename octet_iterator>
			uint32_t prior(octet_iterator& it, octet_iterator start)
			{
				octet_iterator end = it;
				while (internal::is_trail(*(--it)))
				if (it < start)
					throw invalid_utf8(*it); // error - no lead byte in the sequence
				octet_iterator temp = it;
				return next(temp, end);
			}

			/// Deprecated in versions that include "prior"
			template <typename octet_iterator>
			uint32_t previous(octet_iterator& it, octet_iterator pass_start)
			{
				octet_iterator end = it;
				while (internal::is_trail(*(--it)))
				if (it == pass_start)
					throw invalid_utf8(*it); // error - no lead byte in the sequence
				octet_iterator temp = it;
				return next(temp, end);
			}

			template <typename octet_iterator, typename distance_type>
			void advance(octet_iterator& it, distance_type n, octet_iterator end)
			{
				for (distance_type i = 0; i < n; ++i)
					next(it, end);
			}

			template <typename octet_iterator>
			typename std::iterator_traits<octet_iterator>::difference_type
				distance(octet_iterator first, octet_iterator last)
			{
					typename std::iterator_traits<octet_iterator>::difference_type dist;
					for (dist = 0; first < last; ++dist)
						next(first, last);
					return dist;
				}

			template <typename u16bit_iterator, typename octet_iterator>
			octet_iterator utf16to8(u16bit_iterator start, u16bit_iterator end, octet_iterator result)
			{
				while (start != end) {
					uint32_t cp = internal::mask16(*start++);
					// Take care of surrogate pairs first
					if (internal::is_surrogate(cp)) {
						if (start != end) {
							uint32_t trail_surrogate = internal::mask16(*start++);
							if (trail_surrogate >= internal::TRAIL_SURROGATE_MIN && trail_surrogate <= internal::TRAIL_SURROGATE_MAX)
								cp = (cp << 10) + trail_surrogate + internal::SURROGATE_OFFSET;
							else
								throw invalid_utf16(static_cast<uint16_t>(trail_surrogate));
						}
						else
							throw invalid_utf16(static_cast<uint16_t>(*start));

					}
					result = append(cp, result);
				}
				return result;
			}

			template <typename u16bit_iterator, typename octet_iterator>
			u16bit_iterator utf8to16(octet_iterator start, octet_iterator end, u16bit_iterator result)
			{
				while (start != end) {
					uint32_t cp = next(start, end);
					if (cp > 0xffff) { //make a surrogate pair
						*result++ = static_cast<uint16_t>((cp >> 10) + internal::LEAD_OFFSET);
						*result++ = static_cast<uint16_t>((cp & 0x3ff) + internal::TRAIL_SURROGATE_MIN);
					}
					else
						*result++ = static_cast<uint16_t>(cp);
				}
				return result;
			}

			template <typename octet_iterator, typename u32bit_iterator>
			octet_iterator utf32to8(u32bit_iterator start, u32bit_iterator end, octet_iterator result)
			{
				while (start != end)
					result = append(*(start++), result);

				return result;
			}

			template <typename octet_iterator, typename u32bit_iterator>
			u32bit_iterator utf8to32(octet_iterator start, octet_iterator end, u32bit_iterator result)
			{
				while (start < end)
					(*result++) = next(start, end);

				return result;
			}

			// The iterator class
			template <typename octet_iterator>
			class iterator : public std::iterator <std::bidirectional_iterator_tag, uint32_t> {
				octet_iterator it;
				octet_iterator range_start;
				octet_iterator range_end;
			public:
				iterator() {}
				explicit iterator(const octet_iterator& octet_it,
					const octet_iterator& _range_start,
					const octet_iterator& _range_end) :
					it(octet_it), range_start(_range_start), range_end(_range_end)
				{
					if (it < range_start || it > range_end)
						throw std::out_of_range("Invalid utf-8 iterator position");
				}
				// the default "big three" are OK
				octet_iterator base() const { return it; }
				uint32_t operator * () const
				{
					octet_iterator temp = it;
					return next(temp, range_end);
				}
				bool operator == (const iterator& rhs) const
				{
					if (range_start != rhs.range_start || range_end != rhs.range_end)
						throw std::logic_error("Comparing utf-8 iterators defined with different ranges");
					return (it == rhs.it);
				}
				bool operator != (const iterator& rhs) const
				{
					return !(operator == (rhs));
				}
				iterator& operator ++ ()
				{
					next(it, range_end);
					return *this;
				}
				iterator operator ++ (int)
				{
					iterator temp = *this;
					next(it, range_end);
					return temp;
				}
				iterator& operator -- ()
				{
					prior(it, range_start);
					return *this;
				}
				iterator operator -- (int)
				{
					iterator temp = *this;
					prior(it, range_start);
					return temp;
				}
			}; // class iterator

			namespace unchecked
			{
				template <typename octet_iterator>
				octet_iterator append(uint32_t cp, octet_iterator result)
				{
					if (cp < 0x80)                        // one octet
						*(result++) = static_cast<char const>(cp);
					else if (cp < 0x800) {                // two octets
						*(result++) = static_cast<char const>((cp >> 6) | 0xc0);
						*(result++) = static_cast<char const>((cp & 0x3f) | 0x80);
					}
					else if (cp < 0x10000) {              // three octets
						*(result++) = static_cast<char const>((cp >> 12) | 0xe0);
						*(result++) = static_cast<char const>(((cp >> 6) & 0x3f) | 0x80);
						*(result++) = static_cast<char const>((cp & 0x3f) | 0x80);
					}
					else {                                // four octets
						*(result++) = static_cast<char const>((cp >> 18) | 0xf0);
						*(result++) = static_cast<char const>(((cp >> 12) & 0x3f) | 0x80);
						*(result++) = static_cast<char const>(((cp >> 6) & 0x3f) | 0x80);
						*(result++) = static_cast<char const>((cp & 0x3f) | 0x80);
					}
					return result;
				}

				template <typename octet_iterator>
				uint32_t next(octet_iterator& it)
				{
					uint32_t cp = internal::mask8(*it);
					typename std::iterator_traits<octet_iterator>::difference_type length = utf8::internal::sequence_length(it);
					switch (length) {
					case 1:
						break;
					case 2:
						it++;
						cp = ((cp << 6) & 0x7ff) + ((*it) & 0x3f);
						break;
					case 3:
						++it;
						cp = ((cp << 12) & 0xffff) + ((internal::mask8(*it) << 6) & 0xfff);
						++it;
						cp += (*it) & 0x3f;
						break;
					case 4:
						++it;
						cp = ((cp << 18) & 0x1fffff) + ((internal::mask8(*it) << 12) & 0x3ffff);
						++it;
						cp += (internal::mask8(*it) << 6) & 0xfff;
						++it;
						cp += (*it) & 0x3f;
						break;
					}
					++it;
					return cp;
				}

				template <typename octet_iterator>
				uint32_t peek_next(octet_iterator it)
				{
					return next(it);
				}

				template <typename octet_iterator>
				uint32_t prior(octet_iterator& it)
				{
					while (internal::is_trail(*(--it)));
					octet_iterator temp = it;
					return next(temp);
				}

				// Deprecated in versions that include prior, but only for the sake of consistency (see utf8::previous)
				template <typename octet_iterator>
				inline uint32_t previous(octet_iterator& it)
				{
					return prior(it);
				}

				template <typename octet_iterator, typename distance_type>
				void advance(octet_iterator& it, distance_type n)
				{
					for (distance_type i = 0; i < n; ++i)
						next(it);
				}

				template <typename octet_iterator>
				typename std::iterator_traits<octet_iterator>::difference_type
					distance(octet_iterator first, octet_iterator last)
				{
						typename std::iterator_traits<octet_iterator>::difference_type dist;
						for (dist = 0; first < last; ++dist)
							next(first);
						return dist;
					}

				template <typename u16bit_iterator, typename octet_iterator>
				octet_iterator utf16to8(u16bit_iterator start, u16bit_iterator end, octet_iterator result)
				{
					while (start != end) {
						uint32_t cp = internal::mask16(*start++);
						// Take care of surrogate pairs first
						if (internal::is_surrogate(cp)) {
							uint32_t trail_surrogate = internal::mask16(*start++);
							cp = (cp << 10) + trail_surrogate + internal::SURROGATE_OFFSET;
						}
						result = append(cp, result);
					}
					return result;
				}

				template <typename u16bit_iterator, typename octet_iterator>
				u16bit_iterator utf8to16(octet_iterator start, octet_iterator end, u16bit_iterator result)
				{
					while (start != end) {
						uint32_t cp = next(start);
						if (cp > 0xffff) { //make a surrogate pair
							*result++ = static_cast<uint16_t>((cp >> 10) + internal::LEAD_OFFSET);
							*result++ = static_cast<uint16_t>((cp & 0x3ff) + internal::TRAIL_SURROGATE_MIN);
						}
						else
							*result++ = static_cast<uint16_t>(cp);
					}
					return result;
				}

				template <typename octet_iterator, typename u32bit_iterator>
				octet_iterator utf32to8(u32bit_iterator start, u32bit_iterator end, octet_iterator result)
				{
					while (start != end)
						result = append(*(start++), result);

					return result;
				}

				template <typename octet_iterator, typename u32bit_iterator>
				u32bit_iterator utf8to32(octet_iterator start, octet_iterator end, u32bit_iterator result)
				{
					while (start < end)
						(*result++) = next(start);

					return result;
				}

				// The iterator class
				template <typename octet_iterator>
				class iterator : public std::iterator <std::bidirectional_iterator_tag, uint32_t> {
					octet_iterator it;
				public:
					iterator() {}
					explicit iterator(const octet_iterator& octet_it) : it(octet_it) {}
					// the default "big three" are OK
					octet_iterator base() const { return it; }
					uint32_t operator * () const
					{
						octet_iterator temp = it;
						return next(temp);
					}
					bool operator == (const iterator& rhs) const
					{
						return (it == rhs.it);
					}
					bool operator != (const iterator& rhs) const
					{
						return !(operator == (rhs));
					}
					iterator& operator ++ ()
					{
						std::advance(it, internal::sequence_length(it));
						return *this;
					}
					iterator operator ++ (int)
					{
						iterator temp = *this;
						std::advance(it, internal::sequence_length(it));
						return temp;
					}
					iterator& operator -- ()
					{
						prior(it);
						return *this;
					}
					iterator operator -- (int)
					{
						iterator temp = *this;
						prior(it);
						return temp;
					}
				}; // class iterator

			} // namespace utf8::unchecked
		} // namespace utf8
	}
	// \endcond

	namespace fmt
	{
		namespace consts
		{
			static const char			kFormatSpecifierEscapeChar = '%';
			static const std::string	kZeroPaddingStr = std::string("0");
		}

		bool is_digit(char c)
		{
			return c >= '0' && c <= '9';
		}

		bool wild_card_match(const char* str, const char* pattern)
		{
			while (*pattern)
			{
				switch (*pattern)
				{
				case '?':
					if (!*str) return false;
					++str;
					++pattern;
					break;
				case '*':
					if (wild_card_match(str, pattern + 1)) return true;
					if (*str && wild_card_match(str + 1, pattern)) return true;
					return false;
					break;
				default:
					if (*str++ != *pattern++) return false;
					break;
				}
			}
			return !*str && !*pattern;
		}

		std::string ltrim(std::string str)
		{
			str.erase(str.begin(), std::find_if(str.begin(), str.end(), std::not1(std::ptr_fun<int, int>(&std::isspace))));
			return str;
		}

		std::string rtrim(std::string str)
		{
			str.erase(std::find_if(str.rbegin(), str.rend(), std::not1(std::ptr_fun<int, int>(&std::isspace))).base(), str.end());
			return str;
		}

		std::string trim(std::string str)
		{
			return ltrim(rtrim(str));
		}

		std::string lstrip(std::string str, std::string what)
		{
			auto pos = str.find(what);
			if (0 == pos)
			{
				str.erase(pos, what.length());
			}
			return str;
		}

		std::string rstrip(std::string str, std::string what)
		{
			auto pos = str.rfind(what);
			if (str.length() - what.length() == pos)
			{
				str.erase(pos, what.length());
			}
			return str;
		}

		std::string lskip(std::string str, std::string delim)
		{
			auto pos = str.find(delim);
			if (pos == std::string::npos)
			{
				str = std::string();
			}
			else
			{
				str = str.substr(pos + 1);
			}
			return str;
		}

		std::string rskip(std::string str, std::string delim)
		{
			auto pos = str.rfind(delim);
			if (pos == 0)
			{
				str = std::string();
			}
			else if (std::string::npos != pos)
			{
				str = str.substr(0, pos);
			}
			return str;
		}

		std::string rskip_all(std::string str, std::string delim)
		{
			auto pos = str.find(delim);
			if (pos == std::string::npos)
			{
				return str;
			}
			else
			{
				return str.substr(0, pos);
			}
		}

		std::vector<std::string> split(const std::string s, char delim)
		{
			std::vector<std::string> elems;
			std::istringstream ss(s);
			std::string item;
			while (std::getline(ss, item, delim))
			{
				if (!item.empty()) elems.push_back(item);
			}
			return elems;
		}

		std::vector<std::string> split(const std::string s, std::string delim)
		{
			std::vector<std::string> elems;
			std::string ss(s);
			std::string item;
			size_t pos = 0;
			while ((pos = ss.find(delim)) != std::string::npos) {
				item = ss.substr(0, pos);
				if (!item.empty()) elems.push_back(item);
				ss.erase(0, pos + delim.length());
			}
			if (!ss.empty()) elems.push_back(ss);
			return elems;
		}

		std::vector<std::string> split_multi_delims(const std::string s, std::string delims)
		{
			std::vector<std::string> elems;
			std::string ss(s);
			std::string item;
			size_t pos = 0;
			while ((pos = ss.find_first_of(delims)) != std::string::npos) {
				item = ss.substr(0, pos);
				if (!item.empty()) elems.push_back(item);
				ss.erase(0, pos + 1);
			}
			if (!ss.empty()) elems.push_back(ss);
			return elems;
		}

		std::vector<std::string> split_whitespace(const std::string s)
		{
			auto list = split_multi_delims(s, " \t\n");
			std::vector<std::string> ret;
			for (auto elem : list)
			{
				auto rest = fmt::trim(elem);
				if (!rest.empty()) ret.push_back(rest);
			}
			return ret;
		}

		std::pair<std::string, std::string> split_first_occurance(const std::string s, char delim)
		{
			auto pos = s.find_first_of(delim);
			std::string first = s.substr(0, pos);
			std::string second = (pos != std::string::npos ? s.substr(pos + 1) : std::string());
			return std::make_pair(first, second);
		}

		std::vector<std::string>& erase_empty(std::vector<std::string> &vec)
		{
			for (auto it = vec.begin(); it != vec.end();)
			{
				if (it->empty())
				{
					it = vec.erase(it);
				}
				else
				{
					++it;
				}
			}
			return vec;
		}

		std::string join(std::vector<std::string> elems, char delim)
		{
			std::string str;
			elems = erase_empty(elems);
			if (elems.empty()) return str;
			str = elems[0];
			for (std::size_t i = 1; i < elems.size(); ++i)
			{
				if (elems[i].empty()) continue;
				str += delim + elems[i];
      }
			return str;
		}

		bool starts_with(const std::string& str, const std::string& start)
		{
			return (str.length() >= start.length()) && (str.compare(0, start.length(), start) == 0);
		}

		bool ends_with(const std::string& str, const std::string& end)
		{
			return (str.length() >= end.length()) && (str.compare(str.length() - end.length(), end.length(), end) == 0);
		}

		std::string& replace_all(std::string& str, char replaceWhat, char replaceWith)
		{
			std::replace(str.begin(), str.end(), replaceWhat, replaceWith);
			return str;
		}

		std::string& replace_all(std::string& str, const std::string& replaceWhat, const std::string& replaceWith)
		{
			if (replaceWhat == replaceWith) return str;
			std::size_t foundAt = std::string::npos;
			while ((foundAt = str.find(replaceWhat, foundAt + 1)) != std::string::npos)
			{
				str.replace(foundAt, replaceWhat.length(), replaceWith);
			}
			return str;
		}

		void replace_first_with_escape(std::string &str, const std::string &replaceWhat, const std::string &replaceWith)
		{
			std::size_t foundAt = std::string::npos;
			while ((foundAt = str.find(replaceWhat, foundAt + 1)) != std::string::npos)
			{
				if (foundAt > 0 && str[foundAt - 1] == consts::kFormatSpecifierEscapeChar)
				{
					str.erase(foundAt > 0 ? foundAt - 1 : 0, 1);
					++foundAt;
				}
				else
				{
					str.replace(foundAt, replaceWhat.length(), replaceWith);
					return;
				}
			}
		}

		void replace_all_with_escape(std::string &str, const std::string &replaceWhat, const std::string &replaceWith)
		{
			std::size_t foundAt = std::string::npos;
			while ((foundAt = str.find(replaceWhat, foundAt + 1)) != std::string::npos)
			{
				if (foundAt > 0 && str[foundAt - 1] == consts::kFormatSpecifierEscapeChar)
				{
					str.erase(foundAt > 0 ? foundAt - 1 : 0, 1);
					++foundAt;
				}
				else
				{
					str.replace(foundAt, replaceWhat.length(), replaceWith);
					foundAt += replaceWith.length();
				}
			}
		}

		void replace_sequential_with_escape(std::string &str, const std::string &replaceWhat, const std::vector<std::string> &replaceWith)
		{
			std::size_t foundAt = std::string::npos;
			std::size_t candidatePos = 0;
			while ((foundAt = str.find(replaceWhat, foundAt + 1)) != std::string::npos && replaceWith.size() > candidatePos)
			{
				if (foundAt > 0 && str[foundAt - 1] == consts::kFormatSpecifierEscapeChar)
				{
					str.erase(foundAt > 0 ? foundAt - 1 : 0, 1);
					++foundAt;
				}
				else
				{
					str.replace(foundAt, replaceWhat.length(), replaceWith[candidatePos]);
					foundAt += replaceWith[candidatePos].length();
					++candidatePos;
				}
			}
		}

		bool str_equals(const char* s1, const char* s2)
		{
			if (s1 == nullptr && s2 == nullptr) return true;
			if (s1 == nullptr || s2 == nullptr) return false;
			return std::string(s1) == std::string(s2);	// this is safe, not with strcmp
		}

		//std::string& left_zero_padding(std::string &str, int width)
		//{
		//	int toPad = width - static_cast<int>(str.length());
		//	while (toPad > 0)
		//	{
		//		str = consts::kZeroPaddingStr + str;
		//		--toPad;
		//	}
		//	return str;
		//}

		std::string to_lower_ascii(std::string mixed)
		{
			std::transform(mixed.begin(), mixed.end(), mixed.begin(), ::tolower);
			return mixed;
		}

		std::string to_upper_ascii(std::string mixed)
		{
			std::transform(mixed.begin(), mixed.end(), mixed.begin(), ::toupper);
			return mixed;
		}

		std::u16string utf8_to_utf16(std::string u8str)
		{
			try
			{
				std::u16string ret;
				thirdparty::utf8::utf8to16(u8str.begin(), u8str.end(), std::back_inserter(ret));
				return ret;
			}
			catch (...)
			{
				throw RuntimeException("Invalid UTF-8 string.");
			}
		}

		std::string utf16_to_utf8(std::u16string u16str)
		{
			try
			{
				std::vector<unsigned char> u8vec;
				thirdparty::utf8::utf16to8(u16str.begin(), u16str.end(), std::back_inserter(u8vec));
				auto ptr = reinterpret_cast<char*>(u8vec.data());
				return std::string(ptr, ptr + u8vec.size());
			}
			catch (...)
			{
				throw RuntimeException("Invalid UTF-16 string.");
			}
		}

		std::u32string utf8_to_utf32(std::string u8str)
		{
			try
			{
				std::u32string ret;
				thirdparty::utf8::utf8to32(u8str.begin(), u8str.end(), std::back_inserter(ret));
				return ret;
			}
			catch (...)
			{
				throw RuntimeException("Invalid UTF-8 string.");
			}
		}

		std::string utf32_to_utf8(std::u32string u32str)
		{
			try
			{
				std::vector<unsigned char> u8vec;
				thirdparty::utf8::utf32to8(u32str.begin(), u32str.end(), std::back_inserter(u8vec));
				auto ptr = reinterpret_cast<char*>(u8vec.data());
				return std::string(ptr, ptr + u8vec.size());
			}
			catch (...)
			{
				throw RuntimeException("Invalid UTF-32 string.");
			}
		}

	} // namespace fmt

	namespace fs
	{
		Path::Path(std::string path, bool isAbsolute)
		{
			if (isAbsolute) abspath_ = path;
			else abspath_ = os::absolute_path(path);
		}

		bool Path::empty() const
		{
			return abspath_.empty();
		}

		bool Path::exist() const
		{
			if (empty()) return false;
			return os::path_exists(abspath_, true);
		}

		bool Path::is_file() const
		{
			return os::is_file(abspath_);
		}

		bool Path::is_dir() const
		{
			return os::is_directory(abspath_);
		}

		std::string Path::abs_path() const
		{
			return abspath_;
		}

		std::string Path::relative_path() const
		{
			std::string cwd = os::current_working_directory() + os::path_delim();
			if (fmt::starts_with(abspath_, cwd))
			{
				return fmt::lstrip(abspath_, cwd);
			}
			return relative_path(cwd);
		}

		std::string Path::relative_path(std::string root) const
		{
			if (!fmt::ends_with(root, os::path_delim())) root += os::path_delim();
			if (fmt::starts_with(abspath_, root))
			{
				return fmt::lstrip(abspath_, root);
			}

			auto rootParts = os::path_split(root);
			auto thisParts = os::path_split(abspath_);
			std::size_t commonParts = 0;
			std::size_t size = (std::min)(rootParts.size(), thisParts.size());
			for (std::size_t i = 0; i < size; ++i)
			{
				if (!os::path_identical(rootParts[i], thisParts[i])) break;
				++commonParts;
			}

			if (commonParts == 0)
			{
				return abspath_;
			}

			std::vector<std::string> tmp;
			// traverse back from root, add ../ to path
			for (std::size_t pos = rootParts.size(); pos > commonParts; --pos)
			{
				tmp.push_back("..");
			}
			// forward add parts of this path
			for (std::size_t pos = commonParts; pos < thisParts.size(); ++pos)
			{
				tmp.push_back(thisParts[pos]);
			}
			return os::path_join(tmp);
		}

		std::string Path::filename() const
		{
			if (is_file()) return os::path_split_filename(abspath_);
			return std::string();
		}

		Directory::Directory(std::string root, bool recursive)
			:root_(root), recursive_(recursive)
		{
			resolve();
		}

                Directory::Directory(std::string root, const std::string pattern, bool recursive)
			: root_(root), recursive_(recursive)
		{
			resolve();
			filter(pattern);
		}

                Directory::Directory(std::string root, const std::vector<std::string> patterns, bool recursive)
                        : root_(root), recursive_(recursive)
                {
                        resolve();
                        filter(patterns);
                }

                Directory::Directory(std::string root, const std::vector<const char*> patterns, bool recursive)
                        : root_(root), recursive_(recursive)
                {
                        resolve();
                        filter(patterns);
                }

		bool Directory::is_recursive() const
		{
			return recursive_;
		}

		std::string Directory::root() const
		{
			return root_.abs_path();
		}

		std::vector<Path> Directory::to_list() const
		{
			return paths_;
		}

		void Directory::resolve()
		{
			std::deque<std::string> workList;
			if (root_.is_dir())
			{
				workList.push_back(root_.abs_path());
			}

			while (!workList.empty())
			{
				std::vector<std::string> list = os::list_directory(workList.front());
				workList.pop_front();
				for (auto i = list.begin(); i != list.end(); ++i)
				{
					if (os::is_file(*i))
					{
						paths_.push_back(Path(*i));
					}
					else if (os::is_directory(*i))
					{
						paths_.push_back(Path(*i));
						if (recursive_) workList.push_back(*i);	// add subdir work list
					}
					else
					{
						// this should not happen unless file/dir modified during runtime
						// ignore the error
					}
				}
			}
		}

                void Directory::filter(const std::string pattern)
		{
			std::vector<Path> filtered;
			for (auto entry : paths_)
			{
				if (entry.is_file())
				{
					std::string filename = os::path_split_filename(entry.abs_path());
					if (fmt::wild_card_match(filename.c_str(), pattern.c_str()))
					{
						filtered.push_back(entry);
					}
				}
			}
			paths_ = filtered;	// replace all
		}

                void Directory::filter(const std::vector<std::string> patterns)
                {
                    std::vector<Path> filtered;
                    for (auto entry : paths_)
                    {
                            if (entry.is_file())
                            {
                                    std::string filename = os::path_split_filename(entry.abs_path());
                                    for (auto pattern : patterns)
                                    {
                                        if (fmt::wild_card_match(filename.c_str(), pattern.c_str()))
                                        {
                                                filtered.push_back(entry);
                                        }
                                    }
                            }
                    }
                    paths_ = filtered;	// replace all
                }

                void Directory::filter(const std::vector<const char*> patterns)
                {
                    std::vector<Path> filtered;
                    for (auto entry : paths_)
                    {
                            if (entry.is_file())
                            {
                                    std::string filename = os::path_split_filename(entry.abs_path());
                                    for (auto pattern : patterns)
                                    {
                                        if (fmt::wild_card_match(filename.c_str(), pattern))
                                        {
                                                filtered.push_back(entry);
                                        }
                                    }
                            }
                    }
                    paths_ = filtered;	// replace all
                }

		void Directory::reset()
		{
			resolve();
		}

		FileEditor::FileEditor(std::string filename, bool truncateOrNot, int retryTimes, int retryInterval)
		{
			// use absolute path
			filename_ = os::absolute_path(filename);
			// try open
			this->try_open(retryTimes, retryInterval, truncateOrNot);
		};

		//FileEditor::FileEditor(FileEditor&& other) : filename_(std::move(other.filename_)),
		//	stream_(std::move(other.stream_)),
		//	readPos_(std::move(other.readPos_)),
		//	writePos_(std::move(other.writePos_))
		//{
		//	other.filename_ = std::string();
		//};

		bool FileEditor::open(bool truncateOrNot)
		{
			if (this->is_open()) return true;
			std::ios::openmode mode = std::ios::in | std::ios::out | std::ios::binary;
			if (truncateOrNot)
			{
				mode |= std::ios::trunc;
			}
			else
			{
				mode |= std::ios::app;
			}
			os::fstream_open(stream_, filename_, mode);
			if (this->is_open()) return true;
			return false;
		}

		bool FileEditor::open(const char* filename, bool truncateOrNot, int retryTimes, int retryInterval)
		{
			this->close();
			// use absolute path
			filename_ = os::absolute_path(filename);
			// try open
			return this->try_open(retryTimes, retryInterval, truncateOrNot);
		}

		bool FileEditor::open(std::string filename, bool truncateOrNot, int retryTimes, int retryInterval)
		{
			this->close();
			// use absolute path
			filename_ = os::absolute_path(filename);
			// try open
			return this->try_open(retryTimes, retryInterval, truncateOrNot);
		}

		bool FileEditor::reopen(bool truncateOrNot)
		{
			this->close();
			return this->open(truncateOrNot);
		}

		void FileEditor::close()
		{
			stream_.close();
			stream_.clear();
			// unregister this file
			//detail::FileEditorRegistry::instance().erase(filename_);
		}

		bool FileEditor::try_open(int retryTime, int retryInterval, bool truncateOrNot)
		{
			while (retryTime > 0 && (!this->open(truncateOrNot)))
			{
				time::sleep(retryInterval);
				--retryTime;
			}
			return this->is_open();
		}

		FileReader::FileReader(std::string filename, int retryTimes, int retryInterval)
		{
			// use absolute path
			filename_ = os::absolute_path(filename);
			// try open
			this->try_open(retryTimes, retryInterval);
		}

		//FileReader::FileReader(FileReader&& other) : filename_(std::move(other.filename_)), istream_(std::move(other.istream_))
		//{
		//	other.filename_ = std::string();
		//};

		bool FileReader::open()
		{
			if (this->is_open()) return true;
			this->check_valid();
			os::ifstream_open(istream_, filename_, std::ios::in | std::ios::binary);
			if (this->is_open()) return true;
			return false;
		}

		bool FileReader::try_open(int retryTime, int retryInterval)
		{
			while (retryTime > 0 && (!this->open()))
			{
				time::sleep(retryInterval);
				--retryTime;
			}
			return this->is_open();
		}

		std::size_t FileReader::file_size()
		{
			if (is_open())
			{
				auto curPos = istream_.tellg();
				istream_.seekg(0, istream_.end);
				std::size_t size = static_cast<std::size_t>(istream_.tellg());
				istream_.seekg(curPos);
				return size;
			}
			return 0;
		}

		std::size_t FileReader::count_lines()
		{
			// store current read location
			std::streampos readPtrBackup = istream_.tellg();
			istream_.seekg(std::ios_base::beg);

			const int bufSize = 1024 * 1024;	// using 1MB buffer
			std::vector<char> buf(bufSize);

			std::size_t ct = 0;
			std::streamsize nbuf = 0;
			char last = 0;
			do
			{
				istream_.read(&buf.front(), bufSize);
				nbuf = istream_.gcount();
				for (auto i = 0; i < nbuf; i++)
				{
					last = buf[i];
					if (last == '\n')
					{
						++ct;
					}
				}
			} while (nbuf > 0);

			if (last != '\n') ++ct;

			// restore read position
			istream_.clear();
			istream_.seekg(readPtrBackup);

			return ct;
		}

		std::string FileReader::next_line(bool trimWhitespaces)
		{
			std::string line;

			if (!istream_.good() || !istream_.is_open())
			{
				return line;
			}

			if (!istream_.eof())
			{
				std::getline(istream_, line);
				if (trimWhitespaces) return fmt::trim(line);
				if (line.back() == '\r')
				{
					// LFCR \r\n problem
					line.pop_back();
				}
			}
			return line;
		}

		int FileReader::goto_line(int n)
		{
			if (!istream_.good() || !istream_.is_open())
				return -1;

			istream_.seekg(std::ios::beg);

			if (n < 0)
			{
				throw ArgException("Jumping to a negtive position!");
			}

			if (n == 0)
			{
				return 0;
			}

			int i = 0;
			for (i = 0; i < n - 1; ++i)
			{

				istream_.ignore((std::numeric_limits<std::streamsize>::max)(), '\n');

				if (istream_.eof())
				{
					break;
				}
			}

			return i + 1;
		}

		std::size_t get_file_size(std::string filename)
		{
			std::ifstream fs;
			os::ifstream_open(fs, filename, std::ios::in | std::ios::ate);
			std::size_t size = static_cast<std::size_t>(fs.tellg());
			return size;
		}

	} //namespace fs



	namespace time
	{
		namespace consts
		{
			static const unsigned		kMaxDateTimeLength = 2048;
			static const char			*kDateFractionSpecifier = "%frac";
			static const unsigned	    kDateFractionWidth = 3;
			static const char			*kTimerPrecisionSecSpecifier = "%sec";
			static const char			*kTimerPrecisionMsSpecifier = "%ms";
			static const char			*kTimerPrecisionUsSpecifier = "%us";
			static const char			*kTimerPrecisionNsSpecifier = "%ns";
		}

		DateTime::DateTime()
		{
			auto now = std::chrono::system_clock::now();
			timeStamp_ = std::chrono::system_clock::to_time_t(now);
			calendar_ = os::localtime(timeStamp_);
			fraction_ = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count()
				% math::Pow<10, consts::kDateFractionWidth>::result;
			fractionStr_ = fmt::int_to_zero_pad_str(fraction_, consts::kDateFractionWidth);
		}

		void DateTime::to_local_time()
		{
			calendar_ = os::localtime(timeStamp_);
		}

		void DateTime::to_utc_time()
		{
			calendar_ = os::gmtime(timeStamp_);
		}

		std::string DateTime::to_string(const char *format)
		{
			std::string fmt(format);
			fmt::replace_all_with_escape(fmt, consts::kDateFractionSpecifier, fractionStr_);
			std::vector<char> mbuf(fmt.length() + 100);
			std::size_t size = strftime(mbuf.data(), mbuf.size(), fmt.c_str(), &calendar_);
			while (size == 0)
			{
				if (mbuf.size() > consts::kMaxDateTimeLength)
				{
					return std::string("String size exceed limit!");
				}
				mbuf.resize(mbuf.size() * 2);
				size = strftime(mbuf.data(), mbuf.size(), fmt.c_str(), &calendar_);
			}
			return std::string(mbuf.begin(), mbuf.begin() + size);
		}

		DateTime DateTime::local_time()
		{
			DateTime date;
			return date;
		}

		DateTime DateTime::utc_time()
		{
			DateTime date;
			date.to_utc_time();
			return date;
		}

		Timer::Timer()
		{
			this->reset();
		};

		void Timer::reset()
		{
			timeStamp_ = std::chrono::steady_clock::now();
			elapsed_ = 0;
			paused_ = false;
		}

		void Timer::pause()
		{
			if (paused_) return;
			elapsed_ = elapsed_ns();

			paused_ = true;
		}

		void Timer::resume()
		{
			if (!paused_) return;
			timeStamp_ = std::chrono::steady_clock::now();
			paused_ = false;
		}

		std::size_t	Timer::elapsed_ns()
		{
			if (paused_) return elapsed_;
			return static_cast<std::size_t>(std::chrono::duration_cast<std::chrono::nanoseconds>
				(std::chrono::steady_clock::now() - timeStamp_).count()) + elapsed_;
		}

		std::string Timer::elapsed_ns_str()
		{
			return std::to_string(elapsed_ns());
		}

		std::size_t Timer::elapsed_us()
		{
			return elapsed_ns() / 1000;
		}

		std::string Timer::elapsed_us_str()
		{
			return std::to_string(elapsed_us());
		}

		std::size_t Timer::elapsed_ms()
		{
			return elapsed_ns() / 1000000;
		}

		std::string Timer::elapsed_ms_str()
		{
			return std::to_string(elapsed_ms());
		}

		std::size_t Timer::elapsed_sec()
		{
			return elapsed_ns() / 1000000000;
		}

		std::string Timer::elapsed_sec_str()
		{
			return std::to_string(elapsed_sec());
		}

		double Timer::elapsed_sec_double()
		{
			return static_cast<double>(elapsed_ns()) / 1000000000;
		}

		std::string Timer::to_string(const char *format)
		{
			std::string str(format);
			fmt::replace_all_with_escape(str, consts::kTimerPrecisionSecSpecifier, elapsed_sec_str());
			fmt::replace_all_with_escape(str, consts::kTimerPrecisionMsSpecifier, elapsed_ms_str());
			fmt::replace_all_with_escape(str, consts::kTimerPrecisionUsSpecifier, elapsed_us_str());
			fmt::replace_all_with_escape(str, consts::kTimerPrecisionNsSpecifier, elapsed_ns_str());
			return str;
		}


	} // namespace time



	namespace os
	{
		namespace consts
		{
			static const std::string kEndLineCRLF = std::string("\r\n");
			static const std::string kEndLineLF = std::string("\n");
			static const std::string kNativePathDelimWindows = "\\";
			static const std::string kNativePathDelimPosix = "/";
		}

		int system(const char *const command, const char *const moduleName)
		{
			misc::unused(moduleName);
#if ZUPPLY_OS_WINDOWS
			PROCESS_INFORMATION pi;
			STARTUPINFO si;
			std::memset(&pi, 0, sizeof(PROCESS_INFORMATION));
			std::memset(&si, 0, sizeof(STARTUPINFO));
			GetStartupInfoA(&si);
			si.cb = sizeof(si);
			si.wShowWindow = SW_HIDE;
			si.dwFlags |= SW_HIDE | STARTF_USESHOWWINDOW;
			const BOOL res = CreateProcessA((LPCTSTR)moduleName, (LPTSTR)command, 0, 0, FALSE, 0, 0, 0, &si, &pi);
			if (res) {
				WaitForSingleObject(pi.hProcess, INFINITE);
				CloseHandle(pi.hThread);
				CloseHandle(pi.hProcess);
				return 0;
			}
			else return std::system(command);
#elif ZUPPLY_OS_UNIX
			const unsigned int l = std::strlen(command);
			if (l) {
				char *const ncommand = new char[l + 16];
				std::strncpy(ncommand, command, l);
				std::strcpy(ncommand + l, " 2> /dev/null"); // Make command silent.
				const int out_val = std::system(ncommand);
				delete[] ncommand;
				return out_val;
			}
			else return -1;
#else
			misc::unused(command);
			return -1;
#endif
		}

		std::size_t thread_id()
		{
#if ZUPPLY_OS_WINDOWS
			// It exists because the std::this_thread::get_id() is much slower(espcially under VS 2013)
			return  static_cast<size_t>(::GetCurrentThreadId());
#elif __linux__
			return  static_cast<size_t>(syscall(SYS_gettid));
#else //Default to standard C++11 (OSX and other Unix)
			static std::mutex threadIdMutex;
			static std::map<std::thread::id, std::size_t> threadIdHashmap;
			std::lock_guard<std::mutex> lock(threadIdMutex);
			auto id = std::this_thread::get_id();
			if (threadIdHashmap.count(id) < 1)
			{
				threadIdHashmap[id] = threadIdHashmap.size() + 1;
			}
			return threadIdHashmap[id];
			//return static_cast<size_t>(std::hash<std::thread::id>()(std::this_thread::get_id()));
#endif

		}

		int is_atty()
		{
#if ZUPPLY_OS_WINDOWS
			return _isatty(_fileno(stdout));
#elif ZUPPLY_OS_UNIX
			return isatty(fileno(stdout));
#endif
			return 0;
		}

		std::tm localtime(std::time_t t)
		{
			std::tm temp;
#if ZUPPLY_OS_WINDOWS
			localtime_s(&temp, &t);
			return temp;
#elif ZUPPLY_OS_UNIX
			// POSIX SUSv2 thread safe localtime_r
			return *localtime_r(&t, &temp);
#else
			return temp; // return default tm struct, no idea what it is
#endif
		}

		std::tm gmtime(std::time_t t)
		{
			std::tm temp;
#if ZUPPLY_OS_WINDOWS
			gmtime_s(&temp, &t);
			return temp;
#elif ZUPPLY_OS_UNIX
			// POSIX SUSv2 thread safe gmtime_r
			return *gmtime_r(&t, &temp);
#else
			return temp; // return default tm struct, no idea what it is
#endif
		}

		std::wstring utf8_to_wstring(std::string &u8str)
		{
#if ZUPPLY_OS_WINDOWS
			// windows use 16 bit wstring
			std::u16string u16str = fmt::utf8_to_utf16(u8str);
			return std::wstring(u16str.begin(), u16str.end());
#else
			// otherwise use 32 bit wstring
			std::u32string u32str = fmt::utf8_to_utf32(u8str);
			return std::wstring(u32str.begin(), u32str.end());
#endif
		}

		std::string wstring_to_utf8(std::wstring &wstr)
		{
#if ZUPPLY_OS_WINDOWS
			// windows use 16 bit wstring
			std::u16string u16str(wstr.begin(), wstr.end());
			return fmt::utf16_to_utf8(u16str);
#else
			// otherwise use 32 bit wstring
			std::u32string u32str(wstr.begin(), wstr.end());
			return fmt::utf32_to_utf8(u32str);
#endif
		}

		std::vector<std::string> path_split(std::string path)
		{
#if ZUPPLY_OS_WINDOWS
			std::replace(path.begin(), path.end(), '\\', '/');
			return fmt::split(path, '/');
#else
			return fmt::split(path, '/');
#endif
		}

		std::string path_join(std::vector<std::string> elems)
		{
#if ZUPPLY_OS_WINDOWS
			return fmt::join(elems, '\\');
#else
			return fmt::join(elems, '/');
#endif
		}

		std::string path_split_filename(std::string path)
		{
#if ZUPPLY_OS_WINDOWS
			std::string::size_type pos = fmt::trim(path).find_last_of("/\\");
#else
			std::string::size_type pos = fmt::trim(path).find_last_of("/");
#endif
			if (pos == std::string::npos) return path;
			if (pos != path.length())
			{
				return path.substr(pos + 1);
			}
			return std::string();
		}

		std::string path_split_directory(std::string path)
		{
#if ZUPPLY_OS_WINDOWS
			std::string::size_type pos = fmt::trim(path).find_last_of("/\\");
#else
			std::string::size_type pos = fmt::trim(path).find_last_of("/");
#endif
			if (pos != std::string::npos)
			{
				return path.substr(0, pos);
			}
			return std::string();
		}

		std::string path_split_extension(std::string path)
		{
			std::string filename = path_split_filename(path);
			auto list = fmt::split(filename, '.');
			if (list.size() > 1)
			{
				return list.back();
			}
			return std::string();
		}

		std::string path_split_basename(std::string path)
		{
			std::string filename = path_split_filename(path);
			auto list = fmt::split(filename, '.');
			if (list.size() == 1)
			{
				return list[0];
			}
			else if (list.size() > 1)
			{
				return fmt::join({ list.begin(), list.end() - 1 }, '.');
			}
			return std::string();
		}

		std::string path_append_basename(std::string origPath, std::string whatToAppend)
		{
			std::string newPath = path_join({ path_split_directory(origPath), path_split_basename(origPath) })
				+ whatToAppend;
			std::string ext = path_split_extension(origPath);
			if (!ext.empty())
			{
				newPath += "." + ext;
			}
			return newPath;
		}


		void fstream_open(std::fstream &stream, std::string filename, std::ios::openmode openmode)
		{
			// make sure directory exists for the target file
			create_directory_recursive(path_split_directory(filename));
#if ZUPPLY_OS_WINDOWS
            char* buffer = new char[filename.size()];
            wcstombs(buffer,utf8_to_wstring(filename).c_str(),filename.size());
			stream.open(buffer, openmode);
#else
			stream.open(filename, openmode);
#endif
		}

		void ifstream_open(std::ifstream &stream, std::string filename, std::ios::openmode openmode)
		{
#if ZUPPLY_OS_WINDOWS
            char* buffer = new char[filename.size()];
            wcstombs(buffer,utf8_to_wstring(filename).c_str(),filename.size());
			stream.open(buffer, openmode);
#else
			stream.open(filename, openmode);
#endif
		}

		bool rename(std::string oldName, std::string newName)
		{
#if ZUPPLY_OS_WINDOWS
			return (!_wrename(utf8_to_wstring(oldName).c_str(), utf8_to_wstring(newName).c_str()));
#else
			return (!::rename(oldName.c_str(), newName.c_str()));
#endif
		}

		bool copyfile(std::string src, std::string dst, bool replaceDst)
		{
			if (!replaceDst)
			{
				if (path_exists(dst, true)) return false;
			}
			remove_all(dst);
			std::ifstream  srcf;
			std::fstream  dstf;
			ifstream_open(srcf, src, std::ios::binary);
			fstream_open(dstf, dst, std::ios::binary | std::ios::trunc | std::ios::out);
			dstf << srcf.rdbuf();
			return true;
		}

		bool movefile(std::string src, std::string dst, bool replaceDst)
		{
			if (!replaceDst)
			{
				if (path_exists(dst, true)) return false;
			}
			return os::rename(src, dst);
		}

		bool remove_all(std::string path)
		{
			if (!os::path_exists(path, true)) return true;
			if (os::is_directory(path))
			{
				return remove_dir(path);
			}
			return remove_file(path);
		}

#if ZUPPLY_OS_UNIX
		// for nftw tree walk directory operations, so for unix only
		int nftw_remove(const char *path, const struct stat *sb, int flag, struct FTW *ftwbuf)
		{
			misc::unused(sb);
			misc::unused(flag);
			misc::unused(ftwbuf);
			return ::remove(path);
		}
#endif

		bool remove_dir(std::string path, bool recursive)
		{
			// as long as dir does not exist, return true
			if (!is_directory(path)) return true;
#if ZUPPLY_OS_WINDOWS
			std::wstring root = utf8_to_wstring(path);
			bool            bSubdirectory = false;       // Flag, indicating whether subdirectories have been found
			HANDLE          hFile;                       // Handle to directory
			std::wstring     strFilePath;                 // Filepath
			std::wstring     strPattern;                  // Pattern
			WIN32_FIND_DATAW FileInformation;             // File information


			strPattern = root + L"\\*.*";
			hFile = ::FindFirstFileW(strPattern.c_str(), &FileInformation);
			if (hFile != INVALID_HANDLE_VALUE)
			{
				do
				{
					if (FileInformation.cFileName[0] != '.')
					{
						strFilePath.erase();
						strFilePath = root + L"\\" + FileInformation.cFileName;

						if (FileInformation.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
						{
							if (recursive)
							{
								// Delete subdirectory
								bool iRC = remove_dir(wstring_to_utf8(strFilePath), recursive);
								if (!iRC) return false;
							}
							else
								bSubdirectory = true;
						}
						else
						{
							// Set file attributes
							if (::SetFileAttributesW(strFilePath.c_str(),
								FILE_ATTRIBUTE_NORMAL) == FALSE)
								return false;

							// Delete file
							if (::DeleteFileW(strFilePath.c_str()) == FALSE)
								return false;
						}
					}
				} while (::FindNextFileW(hFile, &FileInformation) == TRUE);

				// Close handle
				::FindClose(hFile);

				DWORD dwError = ::GetLastError();
				if (dwError != ERROR_NO_MORE_FILES)
					return false;
				else
				{
					if (!bSubdirectory)
					{
						// Set directory attributes
						if (::SetFileAttributesW(root.c_str(),
							FILE_ATTRIBUTE_NORMAL) == FALSE)
							return false;

						// Delete directory
						if (::RemoveDirectoryW(root.c_str()) == FALSE)
							return false;
					}
				}
			}

			return true;
#else
			if (recursive) ::nftw(path.c_str(), nftw_remove, 20, FTW_DEPTH);
			else ::remove(path.c_str());
			return (is_directory(path) == false);
#endif
		}

		bool remove_file(std::string path)
		{
#if ZUPPLY_OS_WINDOWS
			_wremove(utf8_to_wstring(path).c_str());
#else
			unlink(path.c_str());
#endif
			return (is_file(path) == false);
		}

		std::string last_error()
		{
#if ZUPPLY_OS_WINDOWS
			DWORD error = GetLastError();
			if (error)
			{
				LPVOID lpMsgBuf;
				DWORD bufLen = FormatMessage(
					FORMAT_MESSAGE_ALLOCATE_BUFFER |
					FORMAT_MESSAGE_FROM_SYSTEM |
					FORMAT_MESSAGE_IGNORE_INSERTS,
					NULL,
					error,
					MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
					(LPTSTR)&lpMsgBuf,
					0, NULL);
				if (bufLen)
				{
					LPCSTR lpMsgStr = (LPCSTR)lpMsgBuf;
					std::string result(lpMsgStr, lpMsgStr + bufLen);

					LocalFree(lpMsgBuf);

					return result;
				}
			}
#else
			char* errStr = strerror(errno);
			if (errStr) return std::string(errStr);
#endif
			return std::string();
		}

		std::string endl()
		{
#if ZUPPLY_OS_WINDOWS
			return consts::kEndLineCRLF;
#else // *nix, OSX, and almost everything else(OS 9 or ealier use CR only, but they are antiques now)
			return consts::kEndLineLF;
#endif
		}

		std::string path_delim()
		{
#if ZUPPLY_OS_WINDOWS
			return consts::kNativePathDelimWindows;
#else // posix
			return consts::kNativePathDelimPosix;
#endif
		}

		std::string current_working_directory()
		{
#if ZUPPLY_OS_WINDOWS
			wchar_t *buffer = nullptr;
			if ((buffer = _wgetcwd(nullptr, 0)) == nullptr)
			{
				// failed
				return std::string(".");
			}
			else
			{
				std::wstring ret(buffer);
				free(buffer);
				return wstring_to_utf8(ret);
			}
#elif _GNU_SOURCE
			char *buffer = realpath(".", nullptr);
			if (buffer == nullptr)
			{
				// failed
				return std::string(".");
			}
			else
			{
				// success
				std::string ret(buffer);
				free(buffer);
				return ret;
			}
#else
			char *buffer = getcwd(nullptr, 0);
			if (buffer == nullptr)
			{
				// failed
				return std::string(".");
			}
			else
			{
				// success
				std::string ret(buffer);
				free(buffer);
				return ret;
			}
#endif
		}

		bool path_exists(std::string path, bool considerFile)
		{
#if ZUPPLY_OS_WINDOWS
			DWORD fileType = GetFileAttributesW(utf8_to_wstring(path).c_str());
			if (fileType == INVALID_FILE_ATTRIBUTES) {
				return false;
			}
			return considerFile ? true : ((fileType & FILE_ATTRIBUTE_DIRECTORY) == 0 ? false : true);
#elif ZUPPLY_OS_UNIX
			struct stat st;
			int ret = stat(path.c_str(), &st);
			return considerFile ? (ret == 0) : S_ISDIR(st.st_mode);
#endif
			return false;
		}

		bool is_file(std::string path)
		{
#if ZUPPLY_OS_WINDOWS
			DWORD fileType = GetFileAttributesW(utf8_to_wstring(path).c_str());
			if (fileType == INVALID_FILE_ATTRIBUTES || ((fileType & FILE_ATTRIBUTE_DIRECTORY) != 0))
			{
				return false;
			}
			return true;
#elif ZUPPLY_OS_UNIX
			struct stat st;
			if (stat(path.c_str(), &st) != 0) return false;
			if (S_ISDIR(st.st_mode)) return false;
			return true;
#endif
			return false;
		}

		bool is_directory(std::string path)
		{
#if ZUPPLY_OS_WINDOWS
			DWORD fileType = GetFileAttributesW(utf8_to_wstring(path).c_str());
			if (fileType == INVALID_FILE_ATTRIBUTES) return false;
			if ((fileType & FILE_ATTRIBUTE_DIRECTORY) != 0) return true;
			return false;
#elif ZUPPLY_OS_UNIX
			struct stat st;
			if (stat(path.c_str(), &st) != 0) return false;
			if (S_ISDIR(st.st_mode)) return true;
			return false;
#endif
			return false;
		}

		bool path_identical(std::string first, std::string second, bool forceCaseSensitve)
		{
#if ZUPPLY_OS_WINDOWS
			if (!forceCaseSensitve)
			{
				return fmt::to_lower_ascii(first) == fmt::to_lower_ascii(second);
			}
			return first == second;
#else
			return first == second;
#endif
		}

		std::string absolute_path(std::string reletivePath)
		{
#if ZUPPLY_OS_WINDOWS
			wchar_t *buffer = nullptr;
			std::wstring widePath = utf8_to_wstring(reletivePath);
			buffer = _wfullpath(buffer, widePath.c_str(), _MAX_PATH);
			if (buffer == nullptr)
			{
				// failed
				return reletivePath;
			}
			else
			{
				std::wstring ret(buffer);
				free(buffer);
				return wstring_to_utf8(ret);
			}
#elif ZUPPLY_OS_UNIX
			char *buffer = realpath(reletivePath.c_str(), nullptr);
			if (buffer == nullptr)
			{
				// failed
				if (ENOENT == errno)
				{
					// try recover manually
					std::string dirtyPath;
					if (fmt::starts_with(reletivePath, "/"))
					{
						// already an absolute path
						dirtyPath = reletivePath;
					}
					else
					{
						dirtyPath = path_join({ current_working_directory(), reletivePath });
					}
					std::vector<std::string> parts = path_split(dirtyPath);
					std::vector<std::string> ret;
					for (auto i = parts.begin(); i != parts.end(); ++i)
					{
						if (*i == ".") continue;
						if (*i == "..")
						{
							if (ret.size() < 1) throw RuntimeException("Invalid path: " + dirtyPath);
							ret.pop_back();
						}
						else
						{
							ret.push_back(*i);
						}
					}
					std::string tmp = path_join(ret);
					if (!fmt::starts_with(tmp, "/")) tmp = "/" + tmp;
					return tmp;
				}
				//still failed
				return reletivePath;
			}
			else
			{
				std::string ret(buffer);
				free(buffer);
				return ret;
			}
#endif
			return std::string();
		}

		bool create_directory(std::string path)
		{
#if ZUPPLY_OS_WINDOWS
			std::wstring widePath = utf8_to_wstring(path);
			int ret = _wmkdir(widePath.c_str());
			if (0 == ret) return true;	// success
			if (EEXIST == ret) return true; // already exists
			return false;
#elif ZUPPLY_OS_UNIX
			// read/write/search permissions for owner and group
			// and with read/search permissions for others
			int status = mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
			if (0 == status) return true; // success
			if (EEXIST == errno) return true; // already exists
			return false;
#endif
			return false;
		}

		bool create_directory_recursive(std::string path)
		{
			std::string tmp = absolute_path(path);
			std::string target = tmp;

			while (!is_directory(tmp))
			{
				if (tmp.empty()) return false;	// invalid path
				tmp = absolute_path(tmp + "/../");
			}

			// tmp is the root from where to build
			auto list = path_split(fmt::lstrip(target, tmp));
			for (auto sub : list)
			{
				tmp = path_join({ tmp, sub });
				if (!create_directory(tmp)) break;
			}
			return is_directory(path);
		}

		std::vector<std::string> list_directory(std::string root)
		{
			std::vector<std::string> ret;
			root = os::absolute_path(root);
			if (os::is_file(root))
			{
				// it's a file, not dir
				return ret;
			}
			if (!os::is_directory(root)) return ret;	// not a dir

#if ZUPPLY_OS_WINDOWS
			std::wstring wroot = os::utf8_to_wstring(root);
			wchar_t dirPath[1024];
			wsprintfW(dirPath, L"%s\\*", wroot.c_str());
			WIN32_FIND_DATAW f;
			HANDLE h = FindFirstFileW(dirPath, &f);
			if (h == INVALID_HANDLE_VALUE) { return ret; }

			do
			{
				const wchar_t *name = f.cFileName;
				if (lstrcmpW(name, L".") == 0 || lstrcmpW(name, L"..") == 0) { continue; }
				std::wstring path = wroot + L"\\" + name;
				ret.push_back(os::wstring_to_utf8(path));
			} while (FindNextFileW(h, &f));
			FindClose(h);
			return ret;
#else
			DIR *dir;
			struct dirent *entry;

			if (!(dir = opendir(root.c_str())))
				return ret;
			if (!(entry = readdir(dir)))
				return ret;

			do {
				if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
					continue;
				std::string path = root + "/" + entry->d_name;
				ret.push_back(path);
			} while ((entry = readdir(dir)));
			closedir(dir);
			return ret;
#endif
			return ret;
		}

	}// namespace os

	namespace cds
	{
		bool RWLockable::Counters::is_waiting_for_write() const {
			return writeClaim_ != writeDone_;
		}

		bool RWLockable::Counters::is_waiting_for_read() const {
			return read_ != 0;
		}

		bool RWLockable::Counters::is_my_turn_to_write(Counters const & claim) const {
			return writeDone_ == claim.writeClaim_ - 1;
		}

		bool RWLockable::Counters::want_to_read(RWLockable::Counters * buf) const {
			if (read_ == UINT16_MAX) {
				return false;
			}
			*buf = *this;
			buf->read_ += 1;
			return true;
		}

		bool RWLockable::Counters::want_to_write(RWLockable::Counters * buf) const {
			if (writeClaim_ == UINT8_MAX) {
				return false;
			}
			*buf = *this;
			buf->writeClaim_ += 1;
			return true;
		}

		RWLockable::Counters RWLockable::Counters::done_reading() const {
			Counters c = *this;
			c.read_ -= 1;
			return c;
		}

		RWLockable::Counters RWLockable::Counters::done_writing() const {
			Counters c = *this;
			c.writeDone_ += 1;
			if (c.writeDone_ == UINT8_MAX) {
				c.writeClaim_ = c.writeDone_ = 0;
			}
			return c;
		}

		RWLock RWLockable::lock_for_read() {
			bool written = false;
			do {
				Counters exp = counters_.load(std::memory_order_acquire);
				do {
					if (exp.is_waiting_for_write()) {
						break;
					}
					Counters claim;
					if (!exp.want_to_read(&claim)) {
						break;
					}
					written = counters_.compare_exchange_weak(
						exp, claim,
						std::memory_order_release,
						std::memory_order_acquire
						);
				} while (!written);
				// todo: if (!written) progressive backoff
			} while (!written);
			return RWLock(this, false);
		}

		RWLock RWLockable::lock_for_write() {
			Counters exp = counters_.load(std::memory_order_acquire);
			Counters claim;
			do {
				while (!exp.want_to_write(&claim)) {
					// todo: progressive backoff
					exp = counters_.load(std::memory_order_acquire);
				}
			} while (!counters_.compare_exchange_weak(
				exp, claim,
				std::memory_order_release,
				std::memory_order_acquire
				));
			while (exp.is_waiting_for_read() || !exp.is_my_turn_to_write(claim)) {
				// todo: progressive backoff
				exp = counters_.load(std::memory_order_acquire);
			}
			return RWLock(this, true);
		}

		void RWLockable::unlock_read() {
			Counters exp = counters_.load(std::memory_order_consume);
			Counters des;
			do {
				des = exp.done_reading();
			} while (!counters_.compare_exchange_weak(
				exp, des,
				std::memory_order_release,
				std::memory_order_consume
				));
		}

		void RWLockable::unlock_write() {
			Counters exp = counters_.load(std::memory_order_consume);
			Counters des;
			do {
				des = exp.done_writing();
			} while (!counters_.compare_exchange_weak(
				exp, des,
				std::memory_order_release,
				std::memory_order_consume
				));
		}

		bool RWLockable::is_lock_free() const {
			return counters_.is_lock_free();
		}

		RWLock::RWLock(RWLockable * const lockable, bool const exclusive)
			: lockable_(lockable)
			, lockType_(exclusive ? LockType::write : LockType::read)
		{}

		RWLock::RWLock()
			: lockable_(nullptr)
			, lockType_(LockType::none)
		{}


		RWLock::RWLock(RWLock&& rhs) {
			lockable_ = rhs.lockable_;
			lockType_ = rhs.lockType_;
			rhs.lockable_ = nullptr;
			rhs.lockType_ = LockType::none;
		}

		RWLock& RWLock::operator =(RWLock&& rhs) {
			std::swap(lockable_, rhs.lockable_);
			std::swap(lockType_, rhs.lockType_);
			return *this;
		}


		RWLock::~RWLock() {
			if (lockable_ == nullptr) {
				return;
			}
			switch (lockType_) {
			case LockType::read:
				lockable_->unlock_read();
				break;
			case LockType::write:
				lockable_->unlock_write();
				break;
			default:
				// do nothing
				break;
			}
		}

		void RWLock::unlock() {
			(*this) = RWLock();
			// emptyLock's dtor will now activate
		}

		RWLock::LockType RWLock::get_lock_type() const {
			return lockType_;
		}
	} // namespace cds
} // end namesapce zz
