import * as _ from 'lodash';
import Bluebird = require('bluebird');
import Course from '../../database/models/course';
import StudentEnrollment from '../../database/models/student-enrollment';
import { BaseError } from 'sequelize';
import NotFoundError from '../../exceptions/not-found-error';
import CourseUnitContent from '../../database/models/course-unit-content';
import CourseTopicContent from '../../database/models/course-topic-content';
import CourseWWTopicQuestion from '../../database/models/course-ww-topic-question';
import rendererHelper, { OutputFormat } from '../../utilities/renderer-helper';
import StudentWorkbook from '../../database/models/student-workbook';
import StudentGrade from '../../database/models/student-grade';
import User from '../../database/models/user';
import logger from '../../utilities/logger';
import sequelize = require('sequelize');
import WrappedError from '../../exceptions/wrapped-error';
import AlreadyExistsError from '../../exceptions/already-exists-error';
import appSequelize from '../../database/app-sequelize';
import { GetTopicsOptions, CourseListOptions, UpdateUnitOptions, UpdateTopicOptions, EnrollByCodeOptions, GetGradesOptions, GetStatisticsOnQuestionsOptions, GetStatisticsOnTopicsOptions, GetStatisticsOnUnitsOptions, GetQuestionOptions, GetQuestionResult, SubmitAnswerOptions, SubmitAnswerResult, FindMissingGradesResult, GetQuestionsOptions, GetQuestionsThatRequireGradesForUserOptions, GetUsersThatRequireGradeForQuestionOptions, CreateGradesForUserEnrollmentOptions, CreateGradesForQuestionOptions, CreateNewStudentGradeOptions, UpdateQuestionOptions, UpdateCourseOptions, MakeProblemNumberAvailableOptions, MakeUnitContentOrderAvailableOptions, MakeTopicContentOrderAvailableOptions, CreateCourseOptions, CreateQuestionsForTopicFromDefFileContentOptions, DeleteQuestionsOptions, DeleteTopicsOptions, DeleteUnitsOptions, GetCalculatedRendererParamsOptions, GetCalculatedRendererParamsResponse, UpdateGradeOptions, DeleteUserEnrollmentOptions, ExtendTopicForUserOptions, GetQuestionRepositoryOptions, ExtendTopicQuestionForUserOptions } from './course-types';
import { Constants } from '../../constants';
import courseRepository from './course-repository';
import { UpdateResult, UpsertResult } from '../../generic-interfaces/sequelize-generic-interfaces';
import curriculumRepository from '../curriculum/curriculum-repository';
import CurriculumUnitContent from '../../database/models/curriculum-unit-content';
import CurriculumTopicContent from '../../database/models/curriculum-topic-content';
import CurriculumWWTopicQuestion from '../../database/models/curriculum-ww-topic-question';
import WebWorkDef, { Problem } from '../../utilities/web-work-def-parser';
import { nameof } from '../../utilities/typescript-helpers';
import Role from '../permissions/roles';
import moment = require('moment');
import RederlyExtendedError from '../../exceptions/rederly-extended-error';
import StudentTopicOverride from '../../database/models/student-topic-override';
import StudentTopicQuestionOverride from '../../database/models/student-topic-question-override';

// When changing to import it creates the following compiling error (on instantiation): This expression is not constructable.
// eslint-disable-next-line @typescript-eslint/no-var-requires
const Sequelize = require('sequelize');

class CourseController {
    getCourseById(id: number): Promise<Course> {
        return Course.findOne({
            where: {
                id,
            },
            include: [{
                model: CourseUnitContent,
                as: 'units',
                include: [{
                    model: CourseTopicContent,
                    as: 'topics',
                    include: [{
                        model: CourseWWTopicQuestion,
                        as: 'questions',
                        required: false,
                        where: {
                            active: true
                        }
                    }],
                    required: false,
                    where: {
                        active: true
                    }
                }],
                required: false,
                where: {
                    active: true
                }
            }],
            order: [
                ['units', 'contentOrder', 'ASC'],
                ['units', 'topics', 'contentOrder', 'ASC'],
                ['units', 'topics', 'questions', 'problemNumber', 'ASC'],
            ]
        });
    }

    getTopicById(id: number, userId?: number): Promise<CourseTopicContent> {
        const include = [];
        if (!_.isNil(userId)) {
            include.push({
                model: StudentTopicOverride,
                as: 'studentTopicOverride',
                attributes: ['userId', 'startDate', 'endDate', 'deadDate'],
                required: false,
                where: {
                    active: true,
                    userId: userId
                }
            });
        }

        return CourseTopicContent.findOne({
            where: {
                id,
            },
            include
        });
    }

    getTopics(options: GetTopicsOptions): Promise<CourseTopicContent[]> {
        const { courseId, isOpen, userId } = options;
        const where: sequelize.WhereOptions = {
            active: true
        };
        const include = [];
        if (!_.isNil(courseId)) {
            include.push({
                model: CourseUnitContent,
                as: 'unit',
                attributes: []
            },
            {
                model: StudentTopicOverride,
                as: 'studentTopicOverride',
                attributes: ['userId', 'startDate', 'endDate', 'deadDate'],
                required: false
            });
            where[`$unit.${CourseUnitContent.rawAttributes.courseId.field}$`] = courseId;
            // where[`$studentTopicOverride.${StudentTopicOverride.rawAttributes.courseTopicContentId.field}$`] = `$${CourseTopicContent.rawAttributes.id.field}`;
        }
        
        if (isOpen) {
            const date = new Date();
            // If no userId is passed, show all active topics and topics with extensions (professor view)
            // TODO: Consider breaking these complex queries into functions that describe their utility.
            if (_.isNil(userId)) {
                where[Sequelize.Op.or] = [
                    {
                        [Sequelize.Op.and]: [
                            {
                                startDate: {
                                    [Sequelize.Op.lte]: date
                                }
                            },
                            {
                                deadDate: {
                                    [Sequelize.Op.gte]: date
                                }
                            },
                        ]
                    },
                    {
                        [Sequelize.Op.and]: [
                            {
                                [`$studentTopicOverride.${StudentTopicOverride.rawAttributes.startDate.field}$`]: {
                                    [Sequelize.Op.lte]: date,
                                }
                            },
                            {
                                [`$studentTopicOverride.${StudentTopicOverride.rawAttributes.deadDate.field}$`]: {
                                    [Sequelize.Op.gte]: date,
                                }
                            }
                        ]
                    }
                ];
            } else {
                // If you have overrides, use the overrides, else use the base daterange (student view)
                where[Sequelize.Op.or] = [
                    {
                        [Sequelize.Op.and]: [
                            {
                                startDate: {
                                    [Sequelize.Op.lte]: date
                                }
                            },
                            {
                                deadDate: {
                                    [Sequelize.Op.gte]: date
                                }
                            },
                            {
                                [`$studentTopicOverride.${StudentTopicOverride.rawAttributes.startDate.field}$`]: {
                                    [Sequelize.Op.is]: null,
                                }
                            },
                            {
                                [`$studentTopicOverride.${StudentTopicOverride.rawAttributes.deadDate.field}$`]: {
                                    [Sequelize.Op.is]: null,
                                }
                            },
                        ]
                    },
                    {
                        [Sequelize.Op.and]: [
                            {
                                [`$studentTopicOverride.${StudentTopicOverride.rawAttributes.startDate.field}$`]: {
                                    [Sequelize.Op.lte]: date,
                                }
                            },
                            {
                                [`$studentTopicOverride.${StudentTopicOverride.rawAttributes.deadDate.field}$`]: {
                                    [Sequelize.Op.gte]: date,
                                }
                            }
                        ]
                    }
                ];
            }

            // Only allow original dates if extension dates are null (i.e., no extension was given).
            // Otherwise, use the extension dates.

        }
        return CourseTopicContent.findAll({
            where,
            include
        });
    }

    getCourses(options: CourseListOptions): Bluebird<Course[]> {
        const where: sequelize.WhereOptions = {};
        const include: sequelize.IncludeOptions[] = [];
        if (options.filter.instructorId !== null && options.filter.instructorId !== undefined) {
            where.instructorId = options.filter.instructorId;
        }

        if (options.filter.enrolledUserId !== null && options.filter.enrolledUserId !== undefined) {
            include.push({
                model: StudentEnrollment,
                attributes: [],
                as: 'enrolledStudents',
            });
            where[`$enrolledStudents.${StudentEnrollment.rawAttributes.userId.field}$`] = options.filter.enrolledUserId;
        }

        return Course.findAll({
            where,
            include,
        });
    }

    async createCourse(options: CreateCourseOptions): Promise<Course> {
        if (options.options.useCurriculum) {
            return appSequelize.transaction(async () => {
                // I didn't want this in the transaction, however use strict throws errors if not
                if (_.isNil(options.object.curriculumId)) {
                    throw new NotFoundError('Cannot useCurriculum if curriculumId is not given');
                }
                const curriculum = await curriculumRepository.getCurriculumById(options.object.curriculumId);
                const createdCourse = await courseRepository.createCourse(options.object);
                await curriculum.units?.asyncForEach(async (curriculumUnit: CurriculumUnitContent) => {
                    if (curriculumUnit.active === false) {
                        logger.warn(`Inactive curriculum unit was fetched in query for create course ID#${curriculumUnit.id}`);
                        return;
                    }
                    const createdCourseUnit = await courseRepository.createUnit({
                        // active: curriculumUnit.active,
                        contentOrder: curriculumUnit.contentOrder,
                        courseId: createdCourse.id,
                        curriculumUnitId: curriculumUnit.id,
                        name: curriculumUnit.name,
                    });
                    await curriculumUnit.topics?.asyncForEach(async (curriculumTopic: CurriculumTopicContent) => {
                        if (curriculumTopic.active === false) {
                            logger.warn(`Inactive curriculum topic was fetched in query for create course ID#${curriculumTopic.id}`);
                            return;
                        }
                        const createdCourseTopic: CourseTopicContent = await courseRepository.createCourseTopic({
                            // active: curriculumTopic.active,
                            curriculumTopicContentId: curriculumTopic.id,
                            courseUnitContentId: createdCourseUnit.id,
                            topicTypeId: curriculumTopic.topicTypeId,
                            name: curriculumTopic.name,
                            contentOrder: curriculumTopic.contentOrder,

                            startDate: createdCourse.end,
                            endDate: createdCourse.end,
                            deadDate: createdCourse.end,
                            partialExtend: false
                        });
                        await curriculumTopic.questions?.asyncForEach(async (curriculumQuestion: CurriculumWWTopicQuestion) => {
                            if (curriculumQuestion.active === false) {
                                logger.warn(`Inactive curriculum question was fetched in query for create course ID#${curriculumQuestion.id}`);
                                return;
                            }
                            await courseRepository.createQuestion({
                                // active: curriculumQuestion.active,
                                courseTopicContentId: createdCourseTopic.id,
                                problemNumber: curriculumQuestion.problemNumber,
                                webworkQuestionPath: curriculumQuestion.webworkQuestionPath,
                                weight: curriculumQuestion.weight,
                                maxAttempts: curriculumQuestion.maxAttempts,
                                hidden: curriculumQuestion.hidden,
                                optional: curriculumQuestion.optional,
                                curriculumQuestionId: curriculumQuestion.id
                            });
                        });
                    });
                });
                return createdCourse;
            });
        } else {
            return courseRepository.createCourse(options.object);
        }
    }

    async createUnit(courseUnitContent: Partial<CourseUnitContent>): Promise<CourseUnitContent> {
        if (_.isNil(courseUnitContent.contentOrder)) {
            if (_.isNil(courseUnitContent.courseId)) {
                throw new Error('We need a course id in order to get a content order');
            }
            courseUnitContent.contentOrder = await courseRepository.getNextContentOrderForCourse(courseUnitContent.courseId);
        }

        if (_.isNil(courseUnitContent.name)) {
            courseUnitContent.name = `Unit #${courseUnitContent.contentOrder}`;
        }
        return courseRepository.createUnit(courseUnitContent);
    }

    async createTopic(courseTopicContent: CourseTopicContent): Promise<CourseTopicContent> {
        if (_.isNil(courseTopicContent.startDate) || _.isNil(courseTopicContent.endDate) || _.isNil(courseTopicContent.deadDate)) {
            if (_.isNil(courseTopicContent.courseUnitContentId)) {
                throw new Error('Cannot assume start, end or dead date if a unit is not supplied');
            }

            const unit = await courseRepository.getCourseUnit({
                id: courseTopicContent.courseUnitContentId
            });

            const course = await unit.getCourse();

            // Date default to end date
            if (_.isNil(courseTopicContent.startDate)) {
                courseTopicContent.startDate = course.end;
            }

            if (_.isNil(courseTopicContent.endDate)) {
                courseTopicContent.endDate = course.end;
            }

            if (_.isNil(courseTopicContent.deadDate)) {
                courseTopicContent.deadDate = course.end;
            }
        }

        if (_.isNil(courseTopicContent.contentOrder)) {
            if (_.isNil(courseTopicContent.courseUnitContentId)) {
                throw new Error('Cannot assume assume content order if a unit is not supplied');
            }
            courseTopicContent.contentOrder = await courseRepository.getNextContentOrderForUnit(courseTopicContent.courseUnitContentId);
        }

        if (_.isNil(courseTopicContent.name)) {
            courseTopicContent.name = `Topic #${courseTopicContent.contentOrder}`;
        }
        return courseRepository.createCourseTopic(courseTopicContent);
    }

    async updateCourse(options: UpdateCourseOptions): Promise<Course[]> {
        const result = await courseRepository.updateCourse(options);
        return result.updatedRecords;
    }

    private async makeCourseTopicOrderAvailable(options: MakeTopicContentOrderAvailableOptions): Promise<UpdateResult<CourseTopicContent>[]> {
        // TODO make this more efficient
        // Currently this updates more records than it has to so that it can remain generic due to time constraints
        // See problem number comment for more details
        const contentOrderField = CourseTopicContent.rawAttributes.contentOrder.field;
        const decrementResult = await courseRepository.updateTopics({
            where: {
                active: true,
                contentOrder: {
                    [Sequelize.Op.gt]: options.sourceContentOrder,
                    // Don't want to mess with the object that was moved out of the way
                    [Sequelize.Op.lt]: Constants.Database.MAX_INTEGER_VALUE
                },
                courseUnitContentId: options.sourceCourseUnitId
            },
            updates: {
                contentOrder: sequelize.literal(`-1 * (${contentOrderField} - 1)`),
            }
        });

        const fixResult = await courseRepository.updateTopics({
            where: {
                active: true,
                contentOrder: {
                    [Sequelize.Op.lt]: 0
                },
            },
            updates: {
                contentOrder: sequelize.literal(`ABS(${contentOrderField})`),
            }
        });

        const incrementResult = await courseRepository.updateTopics({
            where: {
                active: true,
                contentOrder: {
                    [Sequelize.Op.gte]: options.targetContentOrder,
                    // Don't want to mess with the object that was moved out of the way
                    [Sequelize.Op.lt]: Constants.Database.MAX_INTEGER_VALUE
                },
                courseUnitContentId: options.targetCourseUnitId
            },
            updates: {
                contentOrder: sequelize.literal(`-1 * (${contentOrderField} + 1)`),
            }
        });

        const fixResult2 = await courseRepository.updateTopics({
            where: {
                active: true,
                contentOrder: {
                    [Sequelize.Op.lt]: 0
                },
            },
            updates: {
                contentOrder: sequelize.literal(`ABS(${contentOrderField})`),
            }
        });

        return [decrementResult, fixResult, incrementResult, fixResult2];
    }

    async updateTopic(options: UpdateTopicOptions): Promise<CourseTopicContent[]> {
        return appSequelize.transaction(async () => {
            // This is a set of all update results as they come in, since there are 5 updates that occur this will have 5 elements
            let updatesResults: UpdateResult<CourseTopicContent>[] = [];
            if (!_.isNil(options.updates.contentOrder) || !_.isNil(options.updates.courseUnitContentId)) {
                // What happens if you move from one topic to another? Disregarding since that should not be possible from the UI
                const existingTopic = await courseRepository.getCourseTopic({
                    id: options.where.id
                });
                const sourceContentOrder = existingTopic.contentOrder;
                // Move the object out of the way for now, this is due to constraint issues
                // TODO make unique index a deferable unique constraint and then make the transaction deferable
                // NOTE: sequelize did not have a nice way of doing this on unique constraints that use the same key in a composite key
                existingTopic.contentOrder = Constants.Database.MAX_INTEGER_VALUE;;
                await existingTopic.save();
                updatesResults = await this.makeCourseTopicOrderAvailable({
                    sourceContentOrder,
                    sourceCourseUnitId: existingTopic.courseUnitContentId,
                    targetContentOrder: options.updates.contentOrder ?? sourceContentOrder,
                    targetCourseUnitId: options.updates.courseUnitContentId ?? existingTopic.courseUnitContentId
                });
                if (_.isNil(options.updates.contentOrder) && !_.isNil(options.updates.courseUnitContentId)) {
                    options.updates.contentOrder = sourceContentOrder;
                }
            }

            const updateCourseTopicResult = await courseRepository.updateCourseTopic(options);
            updatesResults.push(updateCourseTopicResult);

            // Here we extract the list of updated records
            const updatesResultsUpdatedRecords: CourseTopicContent[][] = updatesResults.map((arr: UpdateResult<CourseTopicContent>) => arr.updatedRecords);
            // Here we come up with a list of all records (they are in the order in which they were updated)
            const updatedRecords: CourseTopicContent[] = new Array<CourseTopicContent>().concat(...updatesResultsUpdatedRecords);
            // Lastly we convert to an object and back to an array so that we only have the last updates
            const resultantUpdates: CourseTopicContent[] = _.chain(updatedRecords)
                .keyBy('id')
                .values()
                .value();
            return resultantUpdates;
        });
    }

    async extendTopicForUser(options: ExtendTopicForUserOptions): Promise<UpsertResult<StudentTopicOverride>> {
        return appSequelize.transaction(() =>  {
            return courseRepository.extendTopicByUser(options);
        });
    }

    private async makeCourseUnitOrderAvailable(options: MakeUnitContentOrderAvailableOptions): Promise<UpdateResult<CourseUnitContent>[]> {
        // TODO make this more efficient
        // Currently this updates more records than it has to so that it can remain generic due to time constraints
        // See problem number comment for more details
        const contentOrderField = CourseUnitContent.rawAttributes.contentOrder.field;
        const decrementResult = await courseRepository.updateUnits({
            where: {
                active: true,
                contentOrder: {
                    [Sequelize.Op.gt]: options.sourceContentOrder,
                    // Don't want to mess with the object that was moved out of the way
                    [Sequelize.Op.lt]: Constants.Database.MAX_INTEGER_VALUE
                },
                courseId: options.sourceCourseId
            },
            updates: {
                contentOrder: sequelize.literal(`-1 * (${contentOrderField} - 1)`),
            }
        });

        const fixResult = await courseRepository.updateUnits({
            where: {
                active: true,
                contentOrder: {
                    [Sequelize.Op.lt]: 0
                },
            },
            updates: {
                contentOrder: sequelize.literal(`ABS(${contentOrderField})`),
            }
        });

        const incrementResult = await courseRepository.updateUnits({
            where: {
                active: true,
                contentOrder: {
                    [Sequelize.Op.gte]: options.targetContentOrder,
                    // Don't want to mess with the object that was moved out of the way
                    [Sequelize.Op.lt]: Constants.Database.MAX_INTEGER_VALUE
                },
                courseId: options.targetCourseId
            },
            updates: {
                contentOrder: sequelize.literal(`-1 * (${contentOrderField} + 1)`),
            }
        });

        const fixResult2 = await courseRepository.updateUnits({
            where: {
                active: true,
                contentOrder: {
                    [Sequelize.Op.lt]: 0
                },
            },
            updates: {
                contentOrder: sequelize.literal(`ABS(${contentOrderField})`),
            }
        });

        return [decrementResult, fixResult, incrementResult, fixResult2];
    }

    async softDeleteQuestions(options: DeleteQuestionsOptions): Promise<UpdateResult<CourseWWTopicQuestion>> {
        let courseTopicContentId = options.courseTopicContentId;
        return appSequelize.transaction(async (): Promise<UpdateResult<CourseWWTopicQuestion>> => {
            const where: sequelize.WhereOptions = _({
                id: options.id,
                courseTopicContentId,
                active: true
            }).omitBy(_.isUndefined).value() as sequelize.WhereOptions;

            // It will always have active, needs more info than that
            if (Object.keys(where).length < 2) {
                throw new Error('Not enough information in where clause');
            }

            let existingQuestion: CourseWWTopicQuestion | null = null;
            if (_.isNil(courseTopicContentId) && !_.isNil(options.id)) {
                existingQuestion = await courseRepository.getQuestion({
                    id: options.id
                });
                courseTopicContentId = existingQuestion.courseTopicContentId;
            }

            if (_.isNil(courseTopicContentId)) {
                throw new Error('Could not figure out course topic content id');
            }

            let problemNumber: number | sequelize.Utils.Literal = await courseRepository.getNextDeletedProblemNumberForTopic(courseTopicContentId);
            if (!_.isNil(courseTopicContentId)) {
                problemNumber = sequelize.literal(`${CourseWWTopicQuestion.rawAttributes.problemNumber.field} + ${problemNumber}`);
            }

            const results: UpdateResult<CourseWWTopicQuestion> = await courseRepository.updateQuestions({
                where,
                updates: {
                    active: false,
                    problemNumber
                }
            });

            if (!_.isNil(existingQuestion)) {
                const problemNumberField = CourseWWTopicQuestion.rawAttributes.problemNumber.field;
                await courseRepository.updateQuestions({
                    where: {
                        active: true,
                        problemNumber: {
                            [Sequelize.Op.gt]: existingQuestion.problemNumber,
                            // Don't want to mess with the object that was moved out of the way
                            [Sequelize.Op.lt]: Constants.Database.MAX_INTEGER_VALUE
                        },
                        courseTopicContentId: existingQuestion.courseTopicContentId
                    },
                    updates: {
                        problemNumber: sequelize.literal(`${problemNumberField} - 1`),
                    }
                });
            }

            return results;
        });
    }

    async softDeleteTopics(options: DeleteTopicsOptions): Promise<UpdateResult<CourseTopicContent>> {
        let courseUnitContentId = options.courseUnitContentId;
        return appSequelize.transaction(async (): Promise<UpdateResult<CourseTopicContent>> => {
            const results: CourseTopicContent[] = [];
            let updatedCount = 0;
            const where: sequelize.WhereOptions = _({
                id: options.id,
                courseUnitContentId: courseUnitContentId,
                active: true
            }).omitBy(_.isUndefined).value() as sequelize.WhereOptions;

            // It will always have active, needs more info than that
            if (Object.keys(where).length < 2) {
                throw new Error('Not enough information in where clause');
            }

            let existingTopic: CourseTopicContent | null = null;
            if (_.isNil(courseUnitContentId) && !_.isNil(options.id)) {
                existingTopic = await courseRepository.getCourseTopic({
                    id: options.id
                });
                courseUnitContentId = existingTopic.courseUnitContentId;
            }

            if (_.isNil(courseUnitContentId)) {
                throw new Error('Could not figure out course unit content id');
            }

            let contentOrder: number | sequelize.Utils.Literal = await courseRepository.getNextDeletedContentOrderForUnit(courseUnitContentId);
            let name: sequelize.Utils.Literal = sequelize.literal(`${CourseTopicContent.rawAttributes[nameof<CourseTopicContent>('name') as string].field} || ${contentOrder}`);
            if (!_.isNil(courseUnitContentId)) {
                const problemNumberLiteralString = `${CourseTopicContent.rawAttributes[nameof<CourseTopicContent>('contentOrder') as string].field} + ${contentOrder}`;
                contentOrder = sequelize.literal(problemNumberLiteralString);
                name = sequelize.literal(`${CourseTopicContent.rawAttributes[nameof<CourseTopicContent>('name') as string].field} || (${problemNumberLiteralString})`);
            }

            const updateCourseTopicResult: UpdateResult<CourseTopicContent> = await courseRepository.updateTopics({
                where,
                updates: {
                    active: false,
                    contentOrder,
                    name
                }
            });

            // TODO should this be returned in the response
            if (!_.isNil(existingTopic)) {
                const contentOrderField = CourseTopicContent.rawAttributes.contentOrder.field;
                await courseRepository.updateTopics({
                    where: {
                        active: true,
                        contentOrder: {
                            [Sequelize.Op.gt]: existingTopic.contentOrder,
                            // Don't want to mess with the object that was moved out of the way
                            [Sequelize.Op.lt]: Constants.Database.MAX_INTEGER_VALUE
                        },
                        courseUnitContentId: existingTopic.courseUnitContentId
                    },
                    updates: {
                        contentOrder: sequelize.literal(`${contentOrderField} - 1`),
                    }
                });
            }

            updatedCount = updateCourseTopicResult.updatedCount;
            await updateCourseTopicResult.updatedRecords.asyncForEach(async (topic: CourseTopicContent) => {
                const result: CourseTopicContent = {
                    ...topic.get({ plain: true }),
                    questions: []
                } as never as CourseTopicContent;

                const questionsResult: UpdateResult<CourseWWTopicQuestion> = await this.softDeleteQuestions({
                    courseTopicContentId: topic.id
                });

                result.questions?.push(...questionsResult.updatedRecords);
                updatedCount += questionsResult.updatedCount;
                results.push(result);
            });
            return {
                updatedCount: updatedCount,
                updatedRecords: results
            };
        });
    }

    async softDeleteUnits(options: DeleteUnitsOptions): Promise<UpdateResult<CourseUnitContent>> {
        return appSequelize.transaction(async (): Promise<UpdateResult<CourseUnitContent>> => {
            const results: CourseUnitContent[] = [];
            let updatedCount = 0;
            const where: sequelize.WhereOptions = _({
                id: options.id,
                active: true
            }).omitBy(_.isUndefined).value() as sequelize.WhereOptions;

            // It will always have active, needs more info than that
            if (Object.keys(where).length < 2) {
                throw new Error('Not enough information in where clause');
            }

            // When deleting multiple units is support this will need to be handled like the other calls
            const existingUnit = await courseRepository.getCourseUnit({
                id: options.id
            });
            const courseId = existingUnit.courseId;

            const contentOrder: number | sequelize.Utils.Literal = await courseRepository.getNextDeletedContentOrderForCourse(courseId);
            const name: sequelize.Utils.Literal = sequelize.literal(`${CourseUnitContent.rawAttributes[nameof<CourseUnitContent>('name') as string].field} || ${contentOrder}`);

            const updateCourseUnitResult = await courseRepository.updateUnits({
                where,
                updates: {
                    active: false,
                    contentOrder,
                    name
                }
            });

            const contentOrderField = CourseUnitContent.rawAttributes.contentOrder.field;
            await courseRepository.updateUnits({
                where: {
                    active: true,
                    contentOrder: {
                        [Sequelize.Op.gt]: existingUnit.contentOrder,
                        // Don't want to mess with the object that was moved out of the way
                        [Sequelize.Op.lt]: Constants.Database.MAX_INTEGER_VALUE
                    },
                    courseId: existingUnit.courseId
                },
                updates: {
                    contentOrder: sequelize.literal(`${contentOrderField} - 1`),
                }
            });


            await updateCourseUnitResult.updatedRecords.asyncForEach(async (unit: CourseUnitContent) => {
                const result: CourseUnitContent = {
                    ...unit.get({ plain: true }),
                    topics: []
                } as never as CourseUnitContent;

                const topicResult: UpdateResult<CourseTopicContent> = await this.softDeleteTopics({
                    courseUnitContentId: unit.id
                });

                result.topics?.push(...topicResult.updatedRecords);
                updatedCount += topicResult.updatedCount;
                results.push(result);
            });

            return {
                updatedCount: updatedCount,
                updatedRecords: results
            };
        });
    }

    async updateCourseUnit(options: UpdateUnitOptions): Promise<CourseUnitContent[]> {
        return appSequelize.transaction(async () => {
            // This is a set of all update results as they come in, since there are 5 updates that occur this will have 5 elements
            let updatesResults: UpdateResult<CourseUnitContent>[] = [];
            if (!_.isNil(options.updates.contentOrder)) {
                // What happens if you move from one topic to another? Disregarding since that should not be possible from the UI
                const existingUnit = await courseRepository.getCourseUnit({
                    id: options.where.id
                });
                const sourceContentOrder = existingUnit.contentOrder;
                // Move the object out of the way for now, this is due to constraint issues
                // TODO make unique index a deferable unique constraint and then make the transaction deferable
                // NOTE: sequelize did not have a nice way of doing this on unique constraints that use the same key in a composite key
                existingUnit.contentOrder = Constants.Database.MAX_INTEGER_VALUE;
                await existingUnit.save();
                updatesResults = await this.makeCourseUnitOrderAvailable({
                    sourceContentOrder,
                    sourceCourseId: existingUnit.courseId,
                    targetContentOrder: options.updates.contentOrder,
                    targetCourseId: options.updates.courseId ?? existingUnit.courseId
                });
            }
            const updateCourseUnitResult = await courseRepository.updateCourseUnit(options);
            updatesResults.push(updateCourseUnitResult);

            // Here we extract the list of updated records
            const updatesResultsUpdatedRecords: CourseUnitContent[][] = updatesResults.map((arr: UpdateResult<CourseUnitContent>) => arr.updatedRecords);
            // Here we come up with a list of all records (they are in the order in which they were updated)
            const updatedRecords: CourseUnitContent[] = new Array<CourseUnitContent>().concat(...updatesResultsUpdatedRecords);
            // Lastly we convert to an object and back to an array so that we only have the last updates
            const resultantUpdates: CourseUnitContent[] = _.chain(updatedRecords)
                .keyBy('id')
                .values()
                .value();
            return resultantUpdates;
        });
    }

    private async makeProblemNumberAvailable(options: MakeProblemNumberAvailableOptions): Promise<UpdateResult<CourseWWTopicQuestion>[]> {
        // TODO make this more efficient
        // Currently this updates more records than it has to so that it can remain generic due to time constraints
        // i.e. if update the problem number from 1 to 1, it will increment and decrement all question in the topic
        // if that problem number update was the only parameter we would not actually make any changes even though it updated all the records
        const problemNumberField = CourseWWTopicQuestion.rawAttributes.problemNumber.field;
        const decrementResult = await courseRepository.updateQuestions({
            where: {
                active: true,
                problemNumber: {
                    [Sequelize.Op.gt]: options.sourceProblemNumber,
                    // Don't want to mess with the object that was moved out of the way
                    [Sequelize.Op.lt]: Constants.Database.MAX_INTEGER_VALUE
                },
                courseTopicContentId: options.sourceTopicId
            },
            updates: {
                problemNumber: sequelize.literal(`-1 * (${problemNumberField} - 1)`),
            }
        });

        const fixResult = await courseRepository.updateQuestions({
            where: {
                active: true,
                problemNumber: {
                    [Sequelize.Op.lt]: 0
                },
            },
            updates: {
                problemNumber: sequelize.literal(`ABS(${problemNumberField})`),
            }
        });

        const incrementResult = await courseRepository.updateQuestions({
            where: {
                active: true,
                problemNumber: {
                    [Sequelize.Op.gte]: options.targetProblemNumber,
                    // Don't want to mess with the object that was moved out of the way
                    [Sequelize.Op.lt]: Constants.Database.MAX_INTEGER_VALUE
                },
                courseTopicContentId: options.targetTopicId
            },
            updates: {
                problemNumber: sequelize.literal(`-1 * (${problemNumberField} + 1)`),
            }
        });

        const fixResult2 = await courseRepository.updateQuestions({
            where: {
                active: true,
                problemNumber: {
                    [Sequelize.Op.lt]: 0
                },
            },
            updates: {
                problemNumber: sequelize.literal(`ABS(${problemNumberField})`),
            }
        });

        return [decrementResult, fixResult, incrementResult, fixResult2];
    }

    updateQuestion(options: UpdateQuestionOptions): Promise<CourseWWTopicQuestion[]> {
        return appSequelize.transaction(async () => {
            // This is a set of all update results as they come in, since there are 5 updates that occur this will have 5 elements
            let updatesResults: UpdateResult<CourseWWTopicQuestion>[] = [];
            if (!_.isNil(options.updates.problemNumber)) {
                // What happens if you move from one topic to another? Disregarding since that should not be possible from the UI
                const existingQuestion = await courseRepository.getQuestion({
                    id: options.where.id
                });
                const sourceProblemNumber = existingQuestion.problemNumber;
                // Move the question out of the way for now, this is due to constraint issues
                // TODO make unique index a deferable unique constraint and then make the transaction deferable
                // NOTE: sequelize did not have a nice way of doing this on unique constraints that use the same key in a composite key
                existingQuestion.problemNumber = Constants.Database.MAX_INTEGER_VALUE;
                await existingQuestion.save();
                updatesResults = await this.makeProblemNumberAvailable({
                    sourceProblemNumber: sourceProblemNumber,
                    sourceTopicId: existingQuestion.courseTopicContentId,
                    targetProblemNumber: options.updates.problemNumber,
                    targetTopicId: options.updates.courseTopicContentId ?? existingQuestion.courseTopicContentId
                });
            }
            const updateQuestionResult = await courseRepository.updateQuestion(options);
            updatesResults.push(updateQuestionResult);

            // Here we extract the list of updated records
            const updatesResultsUpdatedRecords: CourseWWTopicQuestion[][] = updatesResults.map((arr: UpdateResult<CourseWWTopicQuestion>) => arr.updatedRecords);
            // Here we come up with a list of all records (they are in the order in which they were updated)
            const updatedRecords: CourseWWTopicQuestion[] = new Array<CourseWWTopicQuestion>().concat(...updatesResultsUpdatedRecords);
            // Lastly we convert to an object and back to an array so that we only have the last updates
            const resultantUpdates: CourseWWTopicQuestion[] = _.chain(updatedRecords)
                .keyBy('id')
                .values()
                .value();
            return resultantUpdates;
        });
    }

    updateGrade(options: UpdateGradeOptions): Promise<UpdateResult<StudentGrade>> {
        return courseRepository.updateGrade(options);
    }

    async createQuestion(question: Partial<CourseWWTopicQuestion>): Promise<CourseWWTopicQuestion> {
        if (_.isNil(question.problemNumber)) {
            if (_.isNil(question.courseTopicContentId)) {
                throw new Error('Cannot assume problem number if a topic is not provided');
            }
            question.problemNumber = await courseRepository.getNextProblemNumberForTopic(question.courseTopicContentId);
        }
        return courseRepository.createQuestion(question);
    }

    async createQuestionsForTopicFromDefFileContent(options: CreateQuestionsForTopicFromDefFileContentOptions): Promise<CourseWWTopicQuestion[]> {
        const parsedWebworkDef = new WebWorkDef(options.webworkDefFileContent);
        let lastProblemNumber = await courseRepository.getLatestProblemNumberForTopic(options.courseTopicId) || 0;
        return appSequelize.transaction(() => {
            return parsedWebworkDef.problems.asyncForEach(async (problem: Problem) => {
                return this.addQuestion({
                    // active: true,
                    courseTopicContentId: options.courseTopicId,
                    problemNumber: ++lastProblemNumber,
                    webworkQuestionPath: problem.source_file,
                    weight: parseInt(problem.value ?? '1'),
                    maxAttempts: parseInt(problem.max_attempts ?? '-1'),
                    hidden: false,
                    optional: false
                });
            });
        });
    }

    async addQuestion(question: Partial<CourseWWTopicQuestion>): Promise<CourseWWTopicQuestion> {
        return await appSequelize.transaction(async () => {
            const result = await this.createQuestion(question);
            await this.createGradesForQuestion({
                questionId: result.id
            });
            return result;
        });
    }

    getQuestionRecord(id: number): Promise<CourseWWTopicQuestion> {
        return courseRepository.getQuestion({
            id
        });
    }

    async getCalculatedRendererParams({
        role,
        topic,
        courseQuestion
    }: GetCalculatedRendererParamsOptions): Promise<GetCalculatedRendererParamsResponse> {
        let showSolutions = role !== Role.STUDENT;
        // Currently we only need this fetch for student, small optimization to not call the db again
        if (!showSolutions) {
            if (_.isNil(topic)) {
                topic = await this.getTopicById(courseQuestion.courseTopicContentId);
            }
            showSolutions = moment(topic.deadDate).add(Constants.Course.SHOW_SOLUTIONS_DELAY_IN_DAYS, 'days').isBefore(moment());
        }
        return {
            outputformat: rendererHelper.getOutputFormatForRole(role),
            permissionLevel: rendererHelper.getPermissionForRole(role),
            showSolutions: Number(showSolutions),
        };
    }

    async extendQuestionForUser(options: ExtendTopicQuestionForUserOptions): Promise<UpsertResult<StudentTopicQuestionOverride>> {
        return appSequelize.transaction(() =>  {
            return courseRepository.extendTopicQuestionByUser(options);
        });
    }

    async getQuestionWithoutRenderer(options: GetQuestionRepositoryOptions): Promise<CourseWWTopicQuestion> {
        return await courseRepository.getQuestion(options);
    }

    async getQuestion(options: GetQuestionOptions): Promise<GetQuestionResult> {
        // grades/statistics may send workbookID => show problem with workbookID.form_data
        // (not enrolled) problem page will send questionID without userID => show problem with no form_data
        // (enrolled) will send questionID with userID => show problem with grades.currentProblemState
        const courseQuestion = await this.getQuestionRecord(options.questionId);

        if (_.isNil(courseQuestion)) {
            throw new NotFoundError('Could not find the question in the database');
        }

        let workbook: StudentWorkbook | null = null;
        if(!_.isNil(options.workbookId)) {
            workbook = await courseRepository.getWorkbookById(options.workbookId);
            // if you requested a workbook then a workbook must be found
            if(_.isNil(workbook)) {
                throw new NotFoundError('Could not find the specified workbook');
            }
        }

        let studentGrade: StudentGrade | null = null;
        // get studentGrade from workbook if workbookID, 
        // otherwise studentGrade from userID + questionID | null
        if(_.isNil(workbook)) {
            studentGrade = await StudentGrade.findOne({
                where: {
                    userId: options.userId,
                    courseWWTopicQuestionId: options.questionId
                }
            });
        } else {
            studentGrade = await workbook.getStudentGrade();
            if (studentGrade.courseWWTopicQuestionId !== options.questionId) {
                throw new NotFoundError('The workbook you have requested does not belong to the question provided');
            }
        }

        // if no workbookID, get the most recent workbook -- come back and delete this?
        // if not enrolled, we don't even want to have workbooks
        // if enrolled, workbooks are requested by ID for grades/statistics
        // if enrolled, studentGrade holds currentProblemState
        // if(_.isNil(workbook)) {
        //     const workbooks = await studentGrade?.getWorkbooks({
        //         limit: 1,
        //         order: [ [ 'createdAt', 'DESC' ]]
        //     });
        //     workbook = workbooks?.[0] || null;
        // }

        // it may be undefined (user not enrolled)
        // GetProblemParameters requires undefined over null
        let formData: {[key: string]: unknown} | undefined = studentGrade?.currentProblemState;
        // at this point, we only have a workbook if a valid workbookID was provided
        // set the formData to match the contents of the workbook
        if(!_.isNil(workbook)) {
            formData = workbook.submitted.form_data;
        }

        // studentGrade is the source of truth
        const randomSeed = _.isNil(studentGrade) ? null : studentGrade.randomSeed;

        const calculatedRendererParameters = await this.getCalculatedRendererParams({
            courseQuestion,
            role: options.role,
        });

        if (options.readonly) {
            calculatedRendererParameters.outputformat = OutputFormat.STATIC;
        }

        let showCorrectAnswers = false;
        if (options.role === Role.PROFESSOR && !_.isNil(workbook)) {
            showCorrectAnswers = true;
        }

        const rendererData = await rendererHelper.getProblem({
            sourceFilePath: courseQuestion.webworkQuestionPath,
            problemSeed: randomSeed,
            formURL: options.formURL,
            numIncorrect: studentGrade?.numAttempts,
            formData: formData,
            showCorrectAnswers,
            ...calculatedRendererParameters
        });
        return {
            // courseQuestion,
            rendererData
        };
    }

    async submitAnswer(options: SubmitAnswerOptions): Promise<SubmitAnswerResult> {
        const studentGrade: StudentGrade | null = await StudentGrade.findOne({
            where: {
                userId: options.userId,
                courseWWTopicQuestionId: options.questionId
            }
        });

        if (_.isNil(studentGrade)) {
            return {
                studentGrade: null,
                studentWorkbook: null
            };
        }

        // Should this go up a level?
        if (_.isNil(options.submitted.form_data.submitAnswers)) {
            return {
                studentGrade,
                studentWorkbook: null
            };
        }
        const question: CourseWWTopicQuestion = await studentGrade.getQuestion(
            {
                include: [{
                        model: StudentTopicQuestionOverride,
                        as: 'studentTopicQuestionOverride',
                        attributes: ['userId', 'maxAttempts'],
                        required: false,
                        where: {
                            active: true,
                            userId: options.userId
                        }
                }]
            }
        );
        
        
        const topic: CourseTopicContent = await question.getTopic({
            include: [{
                model: StudentTopicOverride,
                as: 'studentTopicOverride',
                attributes: ['userId', 'startDate', 'endDate', 'deadDate'],
                required: false,
                where: {
                    active: true,
                    userId: options.userId
                }
            }]
        });
        
        if (topic.studentTopicOverride?.length === 1) {
            // TODO: Fix typing here
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            _.assign(topic, (topic as any).studentTopicOverride[0]);
        }
        
        if (question.studentTopicQuestionOverride?.length === 1) {
            // TODO: Fix typing here
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            _.assign(question, (question as any).studentTopicQuestionOverride[0]);
        }

        if (moment().isBefore(moment(topic.deadDate).add(Constants.Course.SHOW_SOLUTIONS_DELAY_IN_DAYS, 'days')) && studentGrade.overallBestScore !== 1) {
            const overallBestScore = Math.max(studentGrade.overallBestScore, options.score);
            studentGrade.overallBestScore = overallBestScore;
            // TODO if the max number of attempts is 0 then this will update every time
            if (studentGrade.numAttempts === 0) {
                studentGrade.firstAttempts = options.score;
            }
            studentGrade.latestAttempts = options.score;

            if (
                !studentGrade.locked && // The grade was not locked
                (
                    question.maxAttempts <= Constants.Course.INFINITE_ATTEMPT_NUMBER || // There is no limit to the number of attempts
                    studentGrade.numAttempts < question.maxAttempts // They still have attempts left to use
                )
            ) {
                studentGrade.numAttempts++;
                if (moment().isBefore(moment(topic.endDate))) {
                    // Full credit.
                    studentGrade.bestScore = overallBestScore;
                    studentGrade.legalScore = overallBestScore;
                    studentGrade.partialCreditBestScore = overallBestScore;
                    // if it was overwritten to be better use that max value
                    studentGrade.effectiveScore = Math.max(overallBestScore, studentGrade.effectiveScore);
                } else if (moment().isBefore(moment(topic.deadDate))) {
                    // Partial credit
                    const partialCreditScalar = 0.5;
                    const partialCreditScore = ((options.score - studentGrade.legalScore) * partialCreditScalar) + studentGrade.legalScore;
                    studentGrade.partialCreditBestScore = Math.max(partialCreditScore, studentGrade.partialCreditBestScore);
                    studentGrade.bestScore = studentGrade.partialCreditBestScore;
                    studentGrade.effectiveScore = Math.max(partialCreditScore, studentGrade.effectiveScore);
                }
            }
            try {
                return await appSequelize.transaction(async (): Promise<SubmitAnswerResult> => {
                    await studentGrade.save();

                    const submitted = _.cloneDeep(options.submitted);
                    delete submitted.renderedHTML;
                    const studentWorkbook = await StudentWorkbook.create({
                        studentGradeId: studentGrade.id,
                        userId: options.userId,
                        courseWWTopicQuestionId: studentGrade.courseWWTopicQuestionId,
                        randomSeed: studentGrade.randomSeed,
                        submitted,
                        result: options.score,
                        time: new Date()
                    });

                    return {
                        studentGrade,
                        studentWorkbook
                    };
                });
            } catch (e) {
                if (e instanceof RederlyExtendedError === false) {
                    throw new WrappedError(e.message, e);
                } else {
                    throw e;
                }
            }
        }
        return {
            studentGrade,
            studentWorkbook: null
        };
    }

    getCourseByCode(code: string): Promise<Course> {
        return Course.findOne({
            where: {
                code
            }
        });
    }

    private checkStudentEnrollmentError(e: Error): void {
        if (e instanceof BaseError === false) {
            throw new WrappedError(Constants.ErrorMessage.UNKNOWN_APPLICATION_ERROR_MESSAGE, e);
        }

        const databaseError = e as BaseError;
        switch (databaseError.originalAsSequelizeError?.constraint) {
            case StudentEnrollment.constraints.uniqueUserPerCourse:
                throw new AlreadyExistsError('This user is already enrolled in this course');
            case StudentEnrollment.constraints.foreignKeyCourse:
                throw new NotFoundError('The given course could not be found thus we could not enroll the student');
            case StudentEnrollment.constraints.foreignKeyUser:
                throw new NotFoundError('The given user could not be found thus we could not enroll in the class');
            default:
                throw new WrappedError(Constants.ErrorMessage.UNKNOWN_DATABASE_ERROR_MESSAGE, e);
        }
    }

    async createStudentEnrollment(enrollment: Partial<StudentEnrollment>): Promise<StudentEnrollment> {
        try {
            return await StudentEnrollment.create(enrollment);
        } catch (e) {
            this.checkStudentEnrollmentError(e);
            throw new WrappedError(Constants.ErrorMessage.UNKNOWN_APPLICATION_ERROR_MESSAGE, e);
        }
    }

    async enroll(enrollment: CreateGradesForUserEnrollmentOptions): Promise<StudentEnrollment> {
        return await appSequelize.transaction(async () => {
            const result = await this.createStudentEnrollment({
                ...enrollment,
                enrollDate: new Date()
            });
            await this.createGradesForUserEnrollment({
                courseId: enrollment.courseId,
                userId: enrollment.userId,
            });
            return result;
        });
    }

    async enrollByCode(enrollment: EnrollByCodeOptions): Promise<StudentEnrollment> {
        const course = await this.getCourseByCode(enrollment.code);
        if (course === null) {
            throw new NotFoundError('Could not find course with the given code');
        }
        return this.enroll({
            courseId: course.id,
            userId: enrollment.userId,
        });
    }

    // Returns true is successfully deleted the enrollment.
    async softDeleteEnrollment(deEnrollment: DeleteUserEnrollmentOptions): Promise<boolean> {
        return await appSequelize.transaction(async () => {
            const enrollment = await StudentEnrollment.findOne({
                where: {
                    ...deEnrollment
                }
            });

            if (_.isNull(enrollment)) {
                throw new NotFoundError(`Could not find Student ${deEnrollment.userId} to remove from Course ${deEnrollment.courseId}`);
            }
            
            if (enrollment.dropDate) {
                throw new NotFoundError(`Student ${deEnrollment.userId} has already been dropped from Course ${deEnrollment.courseId}`);
            }

            enrollment.dropDate = new Date();
            return await enrollment.save();
        });
    }

    async findMissingGrades(): Promise<FindMissingGradesResult[]> {
        const result = await User.findAll({
            include: [{
                model: StudentEnrollment,
                as: 'courseEnrollments',
                include: [{
                    model: Course,
                    as: 'course',
                    include: [{
                        model: CourseUnitContent,
                        as: 'units',
                        include: [{
                            model: CourseTopicContent,
                            as: 'topics',
                            include: [{
                                model: CourseWWTopicQuestion,
                                as: 'questions',
                                include: [{
                                    model: StudentGrade,
                                    as: 'grades',
                                    required: false,
                                    where: {
                                        userId: {
                                            [Sequelize.Op.eq]: sequelize.literal('"courseEnrollments".user_id')
                                        }
                                    }
                                }]
                            }]
                        }]
                    }]
                }]
            }],
            where: {
                [`$courseEnrollments.course.units.topics.questions.grades.${StudentGrade.rawAttributes.id.field}$`]: {
                    [Sequelize.Op.eq]: null
                },
            }
        });

        const results: FindMissingGradesResult[] = [];
        result.forEach((student: User) => {
            student.courseEnrollments?.forEach((studentEnrollment: StudentEnrollment) => {
                studentEnrollment.course?.units?.forEach((unit: CourseUnitContent) => {
                    unit.topics?.forEach((topic: CourseTopicContent) => {
                        topic.questions?.forEach((question: CourseWWTopicQuestion) => {
                            results.push({
                                student,
                                question,
                            });
                        });
                    });
                });
            });
        });
        return results;
    }

    async syncMissingGrades(): Promise<void> {
        const missingGrades = await this.findMissingGrades();
        logger.info(`Found ${missingGrades.length} missing grades`);
        await missingGrades.asyncForEach(async (missingGrade: FindMissingGradesResult) => {
            await this.createNewStudentGrade({
                userId: missingGrade.student.id,
                courseTopicQuestionId: missingGrade.question.id
            });
        });
    }

    async getGrades(options: GetGradesOptions): Promise<StudentGrade[]> {
        const {
            courseId,
            questionId,
            topicId,
            unitId,
            userId,
        } = options.where;

        const setFilterCount = [
            courseId,
            questionId,
            topicId,
            unitId,
        ].reduce((accumulator, val) => (accumulator || 0) + (!_.isNil(val) && 1 || 0), 0);

        if (setFilterCount !== 1) {
            throw new Error(`One filter must be set but found ${setFilterCount}`);
        }

        // Using strict with typescript results in WhereOptions failing when set to a partial object, casting it as WhereOptions since it works
        const where: sequelize.WhereOptions = _({
            [`$question.topic.unit.course.${Course.rawAttributes.id.field}$`]: courseId,
            [`$question.topic.unit.${CourseUnitContent.rawAttributes.id.field}$`]: unitId,
            [`$question.topic.${CourseTopicContent.rawAttributes.id.field}$`]: topicId,
            [`$question.${CourseWWTopicQuestion.rawAttributes.id.field}$`]: questionId,
            [`$user.${User.rawAttributes.id.field}$`]: userId,
            active: true
        }).omitBy(_.isUndefined).value() as sequelize.WhereOptions;

        const totalProblemCountCalculationString = `COUNT(question.${CourseWWTopicQuestion.rawAttributes.id.field})`;
        const pendingProblemCountCalculationString = `COUNT(CASE WHEN ${StudentGrade.rawAttributes.numAttempts.field} = 0 THEN ${StudentGrade.rawAttributes.numAttempts.field} END)`;
        const masteredProblemCountCalculationString = `COUNT(CASE WHEN ${StudentGrade.rawAttributes.overallBestScore.field} >= 1 THEN ${StudentGrade.rawAttributes.overallBestScore.field} END)`;
        const inProgressProblemCountCalculationString = `${totalProblemCountCalculationString} - ${pendingProblemCountCalculationString} - ${masteredProblemCountCalculationString}`;

        // Include cannot be null or undefined, coerce to empty array
        let includeOthers = false;
        let unitInclude;
        if (includeOthers || _.isNil(courseId) === false) {
            includeOthers = true;
            unitInclude = [{
                model: Course,
                as: 'course',
                attributes: [],
                where: {
                    active: true
                },
            }];
        }

        let topicInclude;
        if (includeOthers || _.isNil(unitId) === false) {
            includeOthers = true;
            topicInclude = [{
                model: CourseUnitContent,
                as: 'unit',
                attributes: [],
                where: {
                    active: true
                },
                include: unitInclude || [],
            }];
        }

        let questionInclude;
        if (includeOthers || _.isNil(topicId) === false) {
            includeOthers = true;
            questionInclude = [{
                model: CourseTopicContent,
                as: 'topic',
                attributes: [],
                where: {
                    active: true
                },
                include: topicInclude || [],
            }];
        }

        let attributes: sequelize.FindAttributeOptions;
        // Group cannot be empty array, use null if there is no group clause
        let group: string[] | undefined = undefined;
        if (_.isNil(questionId) === false) {
            attributes = [
                'id',
                'effectiveScore',
                'numAttempts'
            ];
            // This should already be the case but let's guarentee it
            group = undefined;
        } else {
            // Not follow the rules version
            // const averageScoreAttribute = sequelize.fn('avg', sequelize.col(`${StudentGrade.rawAttributes.overallBestScore.field}`));
            const pointsEarned = `SUM(${StudentGrade.rawAttributes.effectiveScore.field} * "question".${CourseWWTopicQuestion.rawAttributes.weight.field})`;
            const pointsAvailable = `SUM(CASE WHEN "question".${CourseWWTopicQuestion.rawAttributes.optional.field} = FALSE THEN "question".${CourseWWTopicQuestion.rawAttributes.weight.field} ELSE 0 END)`;
            const averageScoreAttribute = sequelize.literal(`
                CASE WHEN ${pointsAvailable} = 0 THEN
                    NULL
                ELSE
                    ${pointsEarned} / ${pointsAvailable}
                END
            `);

            attributes = [
                [averageScoreAttribute, 'average'],
                [sequelize.literal(pendingProblemCountCalculationString), 'pendingProblemCount'],
                [sequelize.literal(masteredProblemCountCalculationString), 'masteredProblemCount'],
                [sequelize.literal(inProgressProblemCountCalculationString), 'inProgressProblemCount'],
            ];
            // TODO This group needs to match the alias below, I'd like to find a better way to do this
            group = [`user.${User.rawAttributes.id.field}`, 
                `user.${User.rawAttributes.firstName.field}`, 
                `user.${User.rawAttributes.lastName.field}`
            ];
        }

        // Filter all grades to only be included if the student has not been dropped.
        const studentGradeInclude = [{
                model: StudentEnrollment,
                as: 'courseEnrollments',
                required: true,
                attributes: [],
                where: {
                    courseId: {
                        [Sequelize.Op.eq]: sequelize.literal(`"user->courseEnrollments".${Course.rawAttributes.id.field}`)
                    },
                    dropDate: null
                }
            }];

        return StudentGrade.findAll({
            // This query must be run raw, otherwise the deduplication logic in Sequelize will force-add the primary key
            // resulting in a group-by error. For more information: https://github.com/sequelize/sequelize/issues/3920
            raw: true,
            // Using raw results in nested objects being represented with . notation, using this will expand it like we see elsewhere
            nest: true,
            include: [{
                model: User,
                as: 'user',
                attributes: ['id', 'firstName', 'lastName'],
                where: {
                    active: true
                },
                include: studentGradeInclude || []
            }, {
                model: CourseWWTopicQuestion,
                as: 'question',
                attributes: [],
                where: {
                    active: true,
                    hidden: false
                },
                include: questionInclude || [],
            }],
            attributes,
            where,
            group
        });
    }

    getStatisticsOnUnits(options: GetStatisticsOnUnitsOptions): Promise<CourseUnitContent[]> {
        const {
            courseId,
            userId,
        } = options.where;

        const { followQuestionRules } = options;

        // Using strict with typescript results in WhereOptions failing when set to a partial object, casting it as WhereOptions since it works
        const where: sequelize.WhereOptions = _({
            active: true,
            courseId,
            [`$topics.questions.grades.${StudentGrade.rawAttributes.userId.field}$`]: userId,
        }).omitBy(_.isNil).value() as sequelize.WhereOptions;

        let averageScoreAttribute;
        if (followQuestionRules) {
            const pointsEarned = `SUM("topics->questions->grades".${StudentGrade.rawAttributes.effectiveScore.field} * "topics->questions".${CourseWWTopicQuestion.rawAttributes.weight.field})`;
            const pointsAvailable = `SUM(CASE WHEN "topics->questions".${CourseWWTopicQuestion.rawAttributes.optional.field} = FALSE THEN "topics->questions".${CourseWWTopicQuestion.rawAttributes.weight.field} ELSE 0 END)`;
            averageScoreAttribute = sequelize.literal(`
                CASE WHEN ${pointsAvailable} = 0 THEN
                    NULL
                ELSE
                    ${pointsEarned} / ${pointsAvailable}
                END
            `);
        } else {
            averageScoreAttribute = sequelize.fn('avg', sequelize.col(`topics.questions.grades.${StudentGrade.rawAttributes.overallBestScore.field}`));
        }

        // const completionPercentAttribute = sequelize.literal(`
        // CASE WHEN COUNT("topics->questions->grades".${StudentGrade.rawAttributes.id.field}) > 0 THEN
        //     count(
        //         CASE WHEN "topics->questions->grades".${StudentGrade.rawAttributes.overallBestScore.field} >= 1 THEN
        //             "topics->questions->grades".${StudentGrade.rawAttributes.id.field}
        //         END
        //     )::FLOAT / count("topics->questions->grades".${StudentGrade.rawAttributes.id.field})
        // ELSE
        //     NULL
        // END`);
        const completionPercentAttribute = sequelize.fn('avg', sequelize.col(`topics.questions.grades.${StudentGrade.rawAttributes.overallBestScore.field}`));


        return CourseUnitContent.findAll({
            where,
            attributes: [
                'id',
                'name',
                [sequelize.fn('avg', sequelize.col(`topics.questions.grades.${StudentGrade.rawAttributes.numAttempts.field}`)), 'averageAttemptedCount'],
                [averageScoreAttribute, 'averageScore'],
                [sequelize.fn('count', sequelize.col(`topics.questions.grades.${StudentGrade.rawAttributes.id.field}`)), 'totalGrades'],
                [sequelize.literal(`count(CASE WHEN "topics->questions->grades".${StudentGrade.rawAttributes.overallBestScore.field} >= 1 THEN "topics->questions->grades".${StudentGrade.rawAttributes.id.field} END)`), 'completedCount'],
                [completionPercentAttribute, 'completionPercent'],
            ],
            include: [{
                model: CourseTopicContent,
                as: 'topics',
                attributes: [],
                where: {
                    active: true
                },
                include: [{
                    model: CourseWWTopicQuestion,
                    as: 'questions',
                    attributes: [],
                    where: {
                        active: true,
                        hidden: false
                    },
                    include: [{
                        model: StudentGrade,
                        as: 'grades',
                        attributes: [],
                        where: {
                            active: true,
                        }
                    }]
                }]
            }],
            group: [`${CourseUnitContent.name}.${CourseUnitContent.rawAttributes.id.field}`, `${CourseUnitContent.name}.${CourseUnitContent.rawAttributes.id.field}`],
            order: [
                ['contentOrder', 'asc']
            ],
        });
    }

    getStatisticsOnTopics(options: GetStatisticsOnTopicsOptions): Promise<CourseTopicContent[]> {
        const {
            courseUnitContentId,
            courseId,
            userId,
        } = options.where;

        const { followQuestionRules } = options;

        // Using strict with typescript results in WhereOptions failing when set to a partial object, casting it as WhereOptions since it works
        const where: sequelize.WhereOptions = _({
            active: true,
            courseUnitContentId,
            [`$unit.${CourseUnitContent.rawAttributes.courseId.field}$`]: courseId,
            [`$questions.grades.${StudentGrade.rawAttributes.userId.field}$`]: userId,
        }).omitBy(_.isNil).value() as sequelize.WhereOptions;

        const include: sequelize.IncludeOptions[] = [{
            model: CourseWWTopicQuestion,
            as: 'questions',
            attributes: [],
            where: {
                active: true,
                hidden: false
            },
            include: [{
                model: StudentGrade,
                as: 'grades',
                attributes: [],
                where: {
                    active: true
                }
            }]
        }];

        if (!_.isNil(courseId)) {
            include.push({
                model: CourseUnitContent,
                as: 'unit',
                attributes: [],
                where: {
                    active: true
                }
            });
        }

        let averageScoreAttribute;
        if (followQuestionRules) {
            const pointsEarned = `SUM("questions->grades".${StudentGrade.rawAttributes.effectiveScore.field} * "questions".${CourseWWTopicQuestion.rawAttributes.weight.field})`;
            const pointsAvailable = `SUM(CASE WHEN "questions".${CourseWWTopicQuestion.rawAttributes.optional.field} = FALSE THEN "questions".${CourseWWTopicQuestion.rawAttributes.weight.field} ELSE 0 END)`;
            averageScoreAttribute = sequelize.literal(`
                CASE WHEN ${pointsAvailable} = 0 THEN
                    NULL
                ELSE
                    ${pointsEarned} / ${pointsAvailable}
                END
            `);
        } else {
            averageScoreAttribute = sequelize.fn('avg', sequelize.col(`questions.grades.${StudentGrade.rawAttributes.overallBestScore.field}`));
        }

        // const completionPercentAttribute = sequelize.literal(`
        // CASE WHEN COUNT("questions->grades".${StudentGrade.rawAttributes.id.field}) > 0 THEN
        //     count(
        //         CASE WHEN "questions->grades".${StudentGrade.rawAttributes.overallBestScore.field} >= 1 THEN
        //             "questions->grades".${StudentGrade.rawAttributes.id.field}
        //         END
        //     )::FLOAT / count("questions->grades".${StudentGrade.rawAttributes.id.field})
        // ELSE
        //     NULL
        // END`);
        const completionPercentAttribute = sequelize.fn('avg', sequelize.col(`questions.grades.${StudentGrade.rawAttributes.overallBestScore.field}`));

        return CourseTopicContent.findAll({
            where,
            attributes: [
                'id',
                'name',
                [sequelize.fn('avg', sequelize.col(`questions.grades.${StudentGrade.rawAttributes.numAttempts.field}`)), 'averageAttemptedCount'],
                [averageScoreAttribute, 'averageScore'],
                [sequelize.fn('count', sequelize.col(`questions.grades.${StudentGrade.rawAttributes.id.field}`)), 'totalGrades'],
                [sequelize.literal(`count(CASE WHEN "questions->grades".${StudentGrade.rawAttributes.overallBestScore.field} >= 1 THEN "questions->grades".${StudentGrade.rawAttributes.id.field} END)`), 'completedCount'],
                [completionPercentAttribute, 'completionPercent'],
            ],
            include,
            group: [`${CourseTopicContent.name}.${CourseTopicContent.rawAttributes.id.field}`, `${CourseTopicContent.name}.${CourseTopicContent.rawAttributes.name.field}`],
            order: [
                ['contentOrder', 'asc']
            ],
        });
    }

    getStatisticsOnQuestions(options: GetStatisticsOnQuestionsOptions): Promise<CourseWWTopicQuestion[]> {
        const {
            courseTopicContentId,
            courseId,
            userId,
        } = options.where;

        const { followQuestionRules } = options;

        // Using strict with typescript results in WhereOptions failing when set to a partial object, casting it as WhereOptions since it works
        const where: sequelize.WhereOptions = _({
            active: true,
            courseTopicContentId,
            [`$topic.unit.${CourseUnitContent.rawAttributes.courseId.field}$`]: courseId,
            [`$grades.${StudentGrade.rawAttributes.userId.field}$`]: userId,
        }).omitBy(_.isNil).value() as sequelize.WhereOptions;

        const include: sequelize.IncludeOptions[] = [{
            model: StudentGrade,
            as: 'grades',
            // only send the student grade down if we are listing for a user
            attributes: _.isNil(userId) ? [] : undefined,
            where: {
                active: true
            }
        }];

        if (!_.isNil(courseId)) {
            include.push({
                model: CourseTopicContent,
                as: 'topic',
                attributes: [],
                where: {
                    active: true
                },
                include: [{
                    model: CourseUnitContent,
                    as: 'unit',
                    attributes: [],
                    where: {
                        active: true
                    }
                }]
            });
        }
        
        let scoreField: sequelize.Utils.Col = sequelize.col(`grades.${StudentGrade.rawAttributes.overallBestScore.field}`);
        if (followQuestionRules) {
            scoreField = sequelize.col(`grades.${StudentGrade.rawAttributes.effectiveScore.field}`);
        }

        const group = [`${CourseWWTopicQuestion.name}.${CourseWWTopicQuestion.rawAttributes.id.field}`];
        // required to send down the user grade, which we only need when fetching for a user
        if (!_.isNil(userId)) {
            group.push(`grades.${StudentGrade.rawAttributes.id.field}`);
        }

        // // When using this for a single students grade, it's either 100% for completed or 0% for anything else, it doesn't really make sense
        // const completionPercentAttribute = sequelize.literal(`
        // CASE WHEN COUNT("grades".${StudentGrade.rawAttributes.id.field}) > 0 THEN
        //     count(
        //         CASE WHEN "grades".${StudentGrade.rawAttributes.bestScore.field} >= 1 THEN
        //             "grades".${StudentGrade.rawAttributes.id.field}
        //         END
        //     )::FLOAT / count("grades".${StudentGrade.rawAttributes.id.field})
        // ELSE
        //     NULL
        // END`);
        const completionPercentAttribute = sequelize.fn('avg', sequelize.col(`grades.${StudentGrade.rawAttributes.overallBestScore.field}`));

        return CourseWWTopicQuestion.findAll({
            where,
            attributes: [
                'id',
                [sequelize.literal(`'Problem ' || "${CourseWWTopicQuestion.name}".${CourseWWTopicQuestion.rawAttributes.problemNumber.field}`), 'name'],
                [sequelize.fn('avg', sequelize.col(`grades.${StudentGrade.rawAttributes.numAttempts.field}`)), 'averageAttemptedCount'],
                [sequelize.fn('avg', scoreField), 'averageScore'],
                [sequelize.fn('count', sequelize.col(`grades.${StudentGrade.rawAttributes.id.field}`)), 'totalGrades'],
                [sequelize.literal(`count(CASE WHEN "grades".${StudentGrade.rawAttributes.bestScore.field} >= 1 THEN "grades".${StudentGrade.rawAttributes.id.field} END)`), 'completedCount'],
                [completionPercentAttribute, 'completionPercent'],
            ],
            include,
            group,
            order: [
                ['problemNumber', 'asc']
            ],
        });
    }

    async getQuestions(options: GetQuestionsOptions): Promise<CourseWWTopicQuestion[]> {
        const {
            courseTopicContentId,
            userId
        } = options;

        try {
            const include: sequelize.IncludeOptions[] = [];
            if (!_.isNil(userId)) {
                include.push({
                    model: StudentGrade,
                    as: 'grades',
                    required: false,
                    where: {
                        userId: userId
                    }
                });
                include.push({
                    model: StudentTopicQuestionOverride,
                    as: 'studentTopicQuestionOverride',
                    attributes: ['userId', 'maxAttempts'],
                    required: false,
                    where: {
                        active: true,
                        userId: userId
                    }
                });
            }

            // Using strict with typescript results in WhereOptions failing when set to a partial object, casting it as WhereOptions since it works
            const where: sequelize.WhereOptions = _({
                courseTopicContentId,
                active: true
            }).omitBy(_.isUndefined).value() as sequelize.WhereOptions;

            const findOptions: sequelize.FindOptions = {
                include,
                where,
                order: [
                    ['problemNumber', 'ASC'],
                ]
            };
            return await CourseWWTopicQuestion.findAll(findOptions);
        } catch (e) {
            throw new WrappedError('Error fetching problems', e);
        }
    }

    /**
     * Get's a list of questions that are missing a grade
     * We can then go and create a course
     */
    async getQuestionsThatRequireGradesForUser(options: GetQuestionsThatRequireGradesForUserOptions): Promise<CourseWWTopicQuestion[]> {
        const { courseId, userId } = options;
        try {
            return await CourseWWTopicQuestion.findAll({
                include: [{
                    model: CourseTopicContent,
                    as: 'topic',
                    required: true,
                    attributes: [],
                    include: [{
                        model: CourseUnitContent,
                        as: 'unit',
                        required: true,
                        attributes: [],
                        // This where is fine here
                        // We just don't want further results to propogate
                        // Also we don't need course in the join, we need to add a relationship to go through course
                        where: {
                            courseId
                        },
                        include: [{
                            model: Course,
                            as: 'course',
                            required: true,
                            attributes: [],
                            include: [{
                                model: StudentEnrollment,
                                as: 'enrolledStudents',
                                required: true,
                                attributes: [],
                            }]
                        }]
                    }]
                }, {
                    model: StudentGrade,
                    as: 'grades',
                    required: false,
                    attributes: [],
                    where: {
                        id: {
                            [Sequelize.Op.eq]: null
                        }
                    }
                }],
                attributes: [
                    'id'
                ],
                where: {
                    ['$topic.unit.course.enrolledStudents.user_id$']: userId
                }
            });
        } catch (e) {
            throw new WrappedError('Could not getQuestionsThatRequireGradesForUser', e);
        }
    }

    /*
    * Get all users that don't have a grade on a question
    * Useful when adding a question to a course that already has enrollments
    */
    async getUsersThatRequireGradeForQuestion(options: GetUsersThatRequireGradeForQuestionOptions): Promise<StudentEnrollment[]> {
        const { questionId } = options;
        try {
            return await StudentEnrollment.findAll({
                include: [{
                    model: Course,
                    as: 'course',
                    required: true,
                    attributes: [],
                    include: [{
                        model: CourseUnitContent,
                        as: 'units',
                        required: true,
                        attributes: [],
                        include: [{
                            model: CourseTopicContent,
                            as: 'topics',
                            required: true,
                            attributes: [],
                            include: [{
                                model: CourseWWTopicQuestion,
                                required: true,
                                as: 'questions',
                                attributes: [],
                                // This where is ok here because we just don't want results to propogate past this point
                                where: {
                                    id: questionId
                                },
                                include: [{
                                    model: StudentGrade,
                                    as: 'grades',
                                    required: false,
                                    attributes: []
                                }]
                            }]
                        }]
                    }]
                }],
                attributes: [
                    'userId'
                ],
                where: {
                    ['$course.units.topics.questions.grades.student_grade_id$']: {
                        [Sequelize.Op.eq]: null
                    }
                }
            });
        } catch (e) {
            throw new WrappedError('Could not getUsersThatRequireGradeForQuestion', e);
        }
    }

    async createGradesForUserEnrollment(options: CreateGradesForUserEnrollmentOptions): Promise<number> {
        const { courseId, userId } = options;
        const results = await this.getQuestionsThatRequireGradesForUser({
            courseId,
            userId
        });
        await results.asyncForEach(async (result) => {
            await this.createNewStudentGrade({
                courseTopicQuestionId: result.id,
                userId: userId
            });
        });
        return results.length;
    }

    async createGradesForQuestion(options: CreateGradesForQuestionOptions): Promise<number> {
        const { questionId } = options;
        const results = await this.getUsersThatRequireGradeForQuestion({
            questionId
        });
        await results.asyncForEach(async (result) => {
            await this.createNewStudentGrade({
                courseTopicQuestionId: questionId,
                userId: result.userId
            });
        });
        return results.length;
    }

    generateRandomSeed(): number {
        return Math.floor(Math.random() * 999999);
    }

    async createNewStudentGrade(options: CreateNewStudentGradeOptions): Promise<StudentGrade> {
        const {
            userId,
            courseTopicQuestionId
        } = options;
        try {
            return await StudentGrade.create({
                userId: userId,
                courseWWTopicQuestionId: courseTopicQuestionId,
                randomSeed: this.generateRandomSeed(),
                bestScore: 0,
                overallBestScore: 0,
                numAttempts: 0,
                firstAttempts: 0,
                latestAttempts: 0,
            });
        } catch (e) {
            throw new WrappedError('Could not create new student grade', e);
        }
    }
}

export const courseController = new CourseController();
export default courseController;
