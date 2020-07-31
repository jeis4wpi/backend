import * as _ from "lodash";
import Bluebird = require('bluebird');
import Course from '../../database/models/course';
import StudentEnrollment from '../../database/models/student-enrollment';
import { ForeignKeyConstraintError } from 'sequelize';
import NotFoundError from '../../exceptions/not-found-error';
import CourseUnitContent from '../../database/models/course-unit-content';
import CourseTopicContent from '../../database/models/course-topic-content';
import CourseWWTopicQuestion from '../../database/models/course-ww-topic-question';
import rendererHelper from '../../utilities/renderer-helper';
import StudentWorkbook from '../../database/models/student-workbook';
import StudentGrade from '../../database/models/student-grade';
import User from '../../database/models/user';
import logger from '../../utilities/logger';
import sequelize = require("sequelize");
import { UniqueConstraintError } from "sequelize";
import WrappedError from "../../exceptions/wrapped-error";
import AlreadyExistsError from "../../exceptions/already-exists-error";
import appSequelize from "../../database/app-sequelize";
// When changing to import it creates the following compiling error (on instantiation): This expression is not constructable.
// eslint-disable-next-line @typescript-eslint/no-var-requires
const Sequelize = require('sequelize');

interface EnrollByCodeOptions {
    code: string;
    userId: number;
}

interface CourseListOptions {
    filter: {
        instructorId?: number;
        enrolledUserId?: number;
    };
}

interface GetQuestionOptions {
    userId: number;
    questionId: number;
}

interface UpdateTopicOptions {
    where: {
        id: number;
    };
    updates: {
        startDate?: Date;
        endDate?: Date;
        deadDate?: Date;
        name?: string;
        active?: boolean;
        partialExtend?: boolean;
    };
}

interface UpdateUnitOptions {
    where: {
        id: number;
    };
    updates: {
        name?: string;
        active?: boolean;
    };
}

interface GetGradesOptions {
    where: {
        courseId?: number;
        unitId?: number;
        topicId?: number;
        questionId?: number;
    };
}

interface GetStatisticsOnUnitsOptions {
    where: {
        courseId?: number;
    };
}

interface GetStatisticsOnTopicsOptions {
    where: {
        courseUnitContentId?: number;
        courseId?: number;
    };
}

interface GetStatisticsOnQuestionsOptions {
    where: {
        courseTopicContentId?: number;
        courseId?: number;
    };
}

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
                    }]
                }]
            }],
            order: [
                ['units', 'contentOrder', 'ASC'],
                ['units', 'topics', 'contentOrder', 'ASC'],
                ['units', 'topics', 'questions', 'problemNumber', 'ASC'],
            ]
        })
    }

    getTopics({ courseId, isOpen }: { courseId?: number; isOpen?: boolean }) {
        let where: any = {}
        const include = [];
        if (!_.isNil(courseId)) {
            include.push({
                model: CourseUnitContent,
                as: 'unit',
                attributes: []
            })
            where[`$unit.${CourseUnitContent.rawAttributes.courseId.field}$`] = courseId
        }

        if (isOpen) {
            const date = new Date()
            where.startDate = {
                [Sequelize.Op.lte]: date
            }

            where.deadDate = {
                [Sequelize.Op.gte]: date
            }
        }
        return CourseTopicContent.findAll({
            where,
            include
        })
    }

    getCourses(options: CourseListOptions): Bluebird<Course[]> {
        // Where is a dynamic sequelize object
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const where: any = {};
        const include = [];
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

    async createCourse(courseObject: Partial<Course>): Promise<Course> {
        try {
            return await Course.create(courseObject);
        } catch (e) {
            if (e instanceof UniqueConstraintError) {
                // The sequelize type as original as error but the error comes back with this additional field
                // To workaround the typescript error we must declare any
                const violatedConstraint = (e.original as any).constraint
                if (violatedConstraint === Course.constraints.uniqueCourseCode) {
                    throw new AlreadyExistsError('A course already exists with this course code')
                }
            } else if (e instanceof ForeignKeyConstraintError) {
                // The sequelize type as original as error but the error comes back with this additional field
                // To workaround the typescript error we must declare any
                const violatedConstraint = (e.original as any).constraint
                if (violatedConstraint === Course.constraints.foreignKeyCurriculum) {
                    throw new NotFoundError('Could not create the course since the given curriculum does not exist');
                }
            }
            throw new WrappedError("Unknown error occurred", e);
        }
    }

    async createUnit(courseUnitContent: Partial<CourseUnitContent>): Promise<CourseUnitContent> {
        try {
            return await CourseUnitContent.create(courseUnitContent);
        } catch (e) {
            if (e instanceof UniqueConstraintError) {
                // The sequelize type as original as error but the error comes back with this additional field
                // To workaround the typescript error we must declare any
                const violatedConstraint = (e.original as any).constraint
                if (violatedConstraint === CourseUnitContent.constraints.uniqueNamePerCourse) {
                    throw new AlreadyExistsError('A unit with that name already exists within this course');
                } else if (violatedConstraint === CourseUnitContent.constraints.unqiueOrderPerCourse) {
                    throw new AlreadyExistsError('A unit already exists with this order');
                }
            } else if (e instanceof ForeignKeyConstraintError) {
                // The sequelize type as original as error but the error comes back with this additional field
                // To workaround the typescript error we must declare any
                const violatedConstraint = (e.original as any).constraint
                if(violatedConstraint === CourseUnitContent.constraints.foreignKeyCourse) {
                    throw new NotFoundError('The given course was not found to create the unit')
                }
            }
            throw new WrappedError("Unknown error occurred", e);
        }
    }

    async createTopic(courseTopicContent: CourseTopicContent): Promise<CourseTopicContent> {
        try {
            return await CourseTopicContent.create(courseTopicContent);
        } catch (e) {
            if (e instanceof UniqueConstraintError) {
                // The sequelize type as original as error but the error comes back with this additional field
                // To workaround the typescript error we must declare any
                const violatedConstraint = (e.original as any).constraint
                if (violatedConstraint === CourseTopicContent.constraints.uniqueNamePerUnit) {
                    throw new AlreadyExistsError('A topic with that name already exists within this unit');
                } else if (violatedConstraint === CourseTopicContent.constraints.uniqueOrderPerUnit) {
                    throw new AlreadyExistsError('A topic already exists with this unit order');
                }
            } else if (e instanceof ForeignKeyConstraintError) {
                // The sequelize type as original as error but the error comes back with this additional field
                // To workaround the typescript error we must declare any
                const violatedConstraint = (e.original as any).constraint
                if (violatedConstraint === CourseTopicContent.constraints.foreignKeyUnit) {
                    throw new NotFoundError('Given unit id')
                } else if (violatedConstraint === CourseTopicContent.constraints.foreignKeyTopicType) {
                    throw new NotFoundError('Invalid topic type provided')
                }

            }
            throw new WrappedError("Unknown error occurred", e);
        }
    }

    async updateTopic(options: UpdateTopicOptions): Promise<number> {
        try {
            const updates = await CourseTopicContent.update(options.updates, {
                where: options.where
            });
            // updates count
            return updates[0];
        } catch (e) {
            if (e instanceof UniqueConstraintError) {
                // The sequelize type as original as error but the error comes back with this additional field
                // To workaround the typescript error we must declare any
                const violatedConstraint = (e.original as any).constraint
                if (violatedConstraint === CourseTopicContent.constraints.uniqueNamePerUnit) {
                    throw new AlreadyExistsError('A topic with that name already exists within this unit');
                } else if (violatedConstraint === CourseTopicContent.constraints.uniqueOrderPerUnit) {
                    throw new AlreadyExistsError('A topic already exists with this unit order');
                }
            }
            throw new WrappedError("Unknown error occurred", e);
        }
    }

    async updateUnit(options: UpdateUnitOptions): Promise<number> {
        try {
            const updates = await CourseUnitContent.update(options.updates, {
                where: options.where
            });
            // updates count
            return updates[0];
        } catch (e) {
            if (e instanceof UniqueConstraintError) {
                // The sequelize type as original as error but the error comes back with this additional field
                // To workaround the typescript error we must declare any
                const violatedConstraint = (e.original as any).constraint
                if (violatedConstraint === CourseUnitContent.constraints.uniqueNamePerCourse) {
                    throw new AlreadyExistsError('A unit with that name already exists within this course');
                } else if (violatedConstraint === CourseUnitContent.constraints.unqiueOrderPerCourse) {
                    throw new AlreadyExistsError('A unit already exists with this order');
                }
            }
            throw new WrappedError("Unknown error occurred", e);
        }
    }

    async createQuestion(question: Partial<CourseWWTopicQuestion>): Promise<CourseWWTopicQuestion> {
        try {
            return await CourseWWTopicQuestion.create(question);
        } catch (e) {
            if (e instanceof UniqueConstraintError) {
                // The sequelize type as original as error but the error comes back with this additional field
                // To workaround the typescript error we must declare any
                const violatedConstraint = (e.original as any).constraint
                if (violatedConstraint === CourseWWTopicQuestion.constraints.uniqueOrderPerTopic) {
                    throw new AlreadyExistsError('A question with this topic order already exists');
                }
            } else if (e instanceof ForeignKeyConstraintError) {
                // The sequelize type as original as error but the error comes back with this additional field
                // To workaround the typescript error we must declare any
                const violatedConstraint = (e.original as any).constraint
                if (violatedConstraint === CourseWWTopicQuestion.constraints.foreignKeyTopic) {
                    throw new NotFoundError('Could not create the question because the given topic does not exist');
                }
            }
            throw new WrappedError("Unknown error occurred", e);
        }
    }

    async addQuestion(question: Partial<CourseWWTopicQuestion>): Promise<CourseWWTopicQuestion> {
        return await appSequelize.transaction(async () => {
            const result = await this.createQuestion(question);
            await this.createGradesForQuestion({
                questionId: result.id
            });
            return result;
        })
    }

    async getQuestion(question: any): Promise<any> {
        const courseQuestion = await CourseWWTopicQuestion.findOne({
            where: {
                id: question.questionId
            }
        });

        let studentGrade: StudentGrade;
        studentGrade = await StudentGrade.findOne({
            where: {
                userId: question.userId,
                courseWWTopicQuestionId: question.questionId
            }
        });

        const randomSeed = _.isNil(studentGrade) ? 666 : studentGrade.randomSeed;

        const rendererData = await rendererHelper.getProblem({
            sourceFilePath: courseQuestion.webworkQuestionPath,
            problemSeed: randomSeed,
            formURL: question.formURL,
        });
        return {
            // courseQuestion,
            rendererData
        }
    }

    async submitAnswer(options: any): Promise<any> {
        const studentGrade = await StudentGrade.findOne({
            where: {
                userId: options.userId,
                courseWWTopicQuestionId: options.questionId
            }
        });

        if(_.isNil(studentGrade)) {
            return {
                studentGrade: null,
                studentWorkbook: null
            }
        }

        const bestScore = Math.max(studentGrade.overallBestScore, options.score);

        studentGrade.bestScore = bestScore;
        studentGrade.overallBestScore = bestScore;
        studentGrade.numAttempts++;
        if (studentGrade.numAttempts === 1) {
            studentGrade.firstAttempts = options.score;
        }
        studentGrade.latestAttempts = options.score;
        await studentGrade.save();

        const studentWorkbook = await StudentWorkbook.create({
            studentGradeId: studentGrade.id,
            userId: options.userId,
            courseWWTopicQuestionId: studentGrade.courseWWTopicQuestionId,
            randomSeed: studentGrade.randomSeed,
            submitted: options.submitted,
            result: options.score,
            time: new Date()
        })

        return {
            studentGrade,
            studentWorkbook
        }
    }

    getCourseByCode(code: string): Promise<Course> {
        return Course.findOne({
            where: {
                code
            }
        })
    }

    async createStudentEnrollment(enrollment: Partial<StudentEnrollment>): Promise<StudentEnrollment> {
        try {
            return await StudentEnrollment.create(enrollment);
        } catch (e) {
            if (e instanceof ForeignKeyConstraintError) {
                throw new NotFoundError('User or course was not found');
            } else if (e instanceof UniqueConstraintError) {
                // The sequelize type as original as error but the error comes back with this additional field
                // To workaround the typescript error we must declare any
                const violatedConstraint = (e.original as any).constraint
                if (violatedConstraint === StudentEnrollment.constraints.uniqueUserPerCourse) {
                    throw new AlreadyExistsError('This user is already enrolled in this course')
                }
            }
            throw new WrappedError('Unknown error occurred', e);
        }
    }

    async enroll(enrollment: Partial<StudentEnrollment>): Promise<StudentEnrollment> {
        return await appSequelize.transaction(async () => {
            const result = await this.createStudentEnrollment(enrollment);
            await this.createGradesForUserEnrollment({
                courseId: enrollment.courseId,
                userId: enrollment.userId
            });
            return result;    
        })
    }

    async enrollByCode(enrollment: EnrollByCodeOptions): Promise<StudentEnrollment> {
        const course = await this.getCourseByCode(enrollment.code);
        if (course === null) {
            throw new NotFoundError('Could not find course with the given code');
        }
        return this.enroll({
            courseId: course.id,
            userId: enrollment.userId,
            enrollDate: new Date(),
            dropDate: new Date()
        } as StudentEnrollment);
    }

    async findMissingGrades(): Promise<any[]> {
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
                                    required: false
                                }]
                            }]
                        }]
                    }]
                }]
            }],
            where: {
                [`$courseEnrollments.course.units.topics.questions.grades.${StudentGrade.rawAttributes.id.field}$`]: {
                    [Sequelize.Op.eq]: null
                }
            }
        });

        const results: any[] = [];
        result.forEach((student: any) => {
            student.courseEnrollments.forEach((studentEnrollment: any) => {
                studentEnrollment.course.units.forEach((unit: any) => {
                    unit.topics.forEach((topic: any) => {
                        topic.questions.forEach((question: any) => {
                            results.push({
                                student,
                                question,
                            });
                        });
                    });
                });
            })
        })
        return results;
    }

    async syncMissingGrades(): Promise<void> {
        const missingGrades = await this.findMissingGrades();
        logger.info(`Found ${missingGrades.length} missing grades`)
        await missingGrades.asyncForEach(async (missingGrade: any) => {
            await this.createNewStudentGrade({
                userId: missingGrade.student.id,
                courseTopicQuestionId: missingGrade.question.id
            })
        });
    }

    async getGrades(options: GetGradesOptions): Promise<StudentGrade[]> {
        const {
            courseId,
            questionId,
            topicId,
            unitId
        } = options.where;

        const setFilterCount = [
            courseId,
            questionId,
            topicId,
            unitId
        ].reduce((accumulator, val) => accumulator + (!_.isNil(val) && 1 || 0), 0);

        if (setFilterCount !== 1) {
            throw new Error(`One filter must be set but found ${setFilterCount}`);
        }

        const where = _({
            [`$question.topic.unit.course.${Course.rawAttributes.id.field}$`]: courseId,
            [`$question.topic.unit.${CourseUnitContent.rawAttributes.id.field}$`]: unitId,
            [`$question.topic.${CourseTopicContent.rawAttributes.id.field}$`]: topicId,
            [`$question.${CourseWWTopicQuestion.rawAttributes.id.field}$`]: questionId,
        }).omitBy(_.isUndefined).value();

        const totalProblemCountCalculationString = `COUNT(question.${CourseWWTopicQuestion.rawAttributes.id.field})`;
        const pendingProblemCountCalculationString = `COUNT(CASE WHEN ${StudentGrade.rawAttributes.numAttempts.field} = 0 THEN ${StudentGrade.rawAttributes.numAttempts.field} END)`;
        const masteredProblemCountCalculationString = `COUNT(CASE WHEN ${StudentGrade.rawAttributes.bestScore.field} >= 1 THEN ${StudentGrade.rawAttributes.bestScore.field} END)`;
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
            }]
        }

        let topicInclude;
        if (includeOthers || _.isNil(unitId) === false) {
            includeOthers = true;
            topicInclude = [{
                model: CourseUnitContent,
                as: 'unit',
                attributes: [],
                include: unitInclude || [],
            }]
        }

        let questionInclude;
        if (includeOthers || _.isNil(topicId) === false) {
            includeOthers = true;
            questionInclude = [{
                model: CourseTopicContent,
                as: 'topic',
                attributes: [],
                include: topicInclude || [],
            }];
        }

        let attributes: sequelize.FindAttributeOptions;
        // Group cannot be empty array, use null if there is no group clause
        let group: sequelize.GroupOption;
        if (_.isNil(questionId) === false) {
            attributes = [
                'id',
                'bestScore',
                'numAttempts'
            ]
            group = null;
        } else {
            attributes = [
                [sequelize.fn('avg', sequelize.col(`${StudentGrade.rawAttributes.bestScore.field}`)), 'average'],
                [sequelize.literal(pendingProblemCountCalculationString), 'pendingProblemCount'],
                [sequelize.literal(masteredProblemCountCalculationString), 'masteredProblemCount'],
                [sequelize.literal(inProgressProblemCountCalculationString), 'inProgressProblemCount'],
            ];
            // TODO This group needs to match the alias below, I'd like to find a better way to do this
            group = [`user.${User.rawAttributes.id.field}`, `user.${User.rawAttributes.firstName.field}`, `user.${User.rawAttributes.lastName.field}`];
        }

        return StudentGrade.findAll({
            include: [{
                model: User,
                as: 'user',
                attributes: ['id', 'firstName', 'lastName']
            }, {
                model: CourseWWTopicQuestion,
                as: 'question',
                attributes: [],
                include: questionInclude || [],
            }],
            attributes,
            where,
            group
        });
    }

    getStatisticsOnUnits(options: GetStatisticsOnUnitsOptions): Promise<CourseUnitContent[]> {
        const {
            courseId
        } = options.where;

        const where = _({
            courseId,
        }).omitBy(_.isNil).value();

        return CourseUnitContent.findAll({
            where,
            attributes: [
                'id',
                'name',
                [sequelize.fn('avg', sequelize.col(`topics.questions.grades.${StudentGrade.rawAttributes.numAttempts.field}`)), 'averageAttemptedCount'],
                [sequelize.fn('avg', sequelize.col(`topics.questions.grades.${StudentGrade.rawAttributes.bestScore.field}`)), 'averageScore'],
                [sequelize.fn('count', sequelize.col(`topics.questions.grades.${StudentGrade.rawAttributes.id.field}`)), 'totalGrades'],
                [sequelize.literal(`count(CASE WHEN "topics->questions->grades".${StudentGrade.rawAttributes.bestScore.field} >= 1 THEN "topics->questions->grades".${StudentGrade.rawAttributes.id.field} END)`), 'completedCount'],
                [sequelize.literal(`CASE WHEN COUNT("topics->questions->grades".${StudentGrade.rawAttributes.id.field}) > 0 THEN count(CASE WHEN "topics->questions->grades".${StudentGrade.rawAttributes.bestScore.field} >= 1 THEN "topics->questions->grades".${StudentGrade.rawAttributes.id.field} END)::FLOAT / count("topics->questions->grades".${StudentGrade.rawAttributes.id.field}) ELSE NULL END`), 'completionPercent'],
            ],
            include: [{
                model: CourseTopicContent,
                as: 'topics',
                attributes: [],
                include: [{
                    model: CourseWWTopicQuestion,
                    as: 'questions',
                    attributes: [],
                    include: [{
                        model: StudentGrade,
                        as: 'grades',
                        attributes: []
                    }]
                }]
            }],
            group: [`${CourseUnitContent.name}.${CourseUnitContent.rawAttributes.id.field}`, `${CourseUnitContent.name}.${CourseUnitContent.rawAttributes.id.field}`]
        })
    }

    getStatisticsOnTopics(options: GetStatisticsOnTopicsOptions): Promise<CourseTopicContent[]> {
        const {
            courseUnitContentId,
            courseId
        } = options.where;

        const where = _({
            courseUnitContentId,
            [`$unit.${CourseUnitContent.rawAttributes.courseId.field}$`]: courseId
        }).omitBy(_.isNil).value();

        const include: sequelize.IncludeOptions[] = [{
            model: CourseWWTopicQuestion,
            as: 'questions',
            attributes: [],
            include: [{
                model: StudentGrade,
                as: 'grades',
                attributes: []
            }]
        }]

        if (!_.isNil(courseId)) {
            include.push({
                model: CourseUnitContent,
                as: 'unit',
                attributes: []
            })
        }


        return CourseTopicContent.findAll({
            where,
            attributes: [
                'id',
                'name',
                [sequelize.fn('avg', sequelize.col(`questions.grades.${StudentGrade.rawAttributes.numAttempts.field}`)), 'averageAttemptedCount'],
                [sequelize.fn('avg', sequelize.col(`questions.grades.${StudentGrade.rawAttributes.bestScore.field}`)), 'averageScore'],
                [sequelize.fn('count', sequelize.col(`questions.grades.${StudentGrade.rawAttributes.id.field}`)), 'totalGrades'],
                [sequelize.literal(`count(CASE WHEN "questions->grades".${StudentGrade.rawAttributes.bestScore.field} >= 1 THEN "questions->grades".${StudentGrade.rawAttributes.id.field} END)`), 'completedCount'],
                [sequelize.literal(`CASE WHEN COUNT("questions->grades".${StudentGrade.rawAttributes.id.field}) > 0 THEN count(CASE WHEN "questions->grades".${StudentGrade.rawAttributes.bestScore.field} >= 1 THEN "questions->grades".${StudentGrade.rawAttributes.id.field} END)::FLOAT / count("questions->grades".${StudentGrade.rawAttributes.id.field}) ELSE NULL END`), 'completionPercent'],
            ],
            include,
            group: [`${CourseTopicContent.name}.${CourseTopicContent.rawAttributes.id.field}`, `${CourseTopicContent.name}.${CourseTopicContent.rawAttributes.name.field}`]
        })
    }

    getStatisticsOnQuestions(options: GetStatisticsOnQuestionsOptions): Promise<CourseWWTopicQuestion[]> {
        const {
            courseTopicContentId,
            courseId
        } = options.where;

        const where = _({
            courseTopicContentId,
            [`$topic.unit.${CourseUnitContent.rawAttributes.courseId.field}$`]: courseId
        }).omitBy(_.isNil).value();

        const include: sequelize.IncludeOptions[] = [{
            model: StudentGrade,
            as: 'grades',
            attributes: []
        }]

        if (!_.isNil(courseId)) {
            include.push({
                model: CourseTopicContent,
                as: 'topic',
                attributes: [],
                include: [{
                    model: CourseUnitContent,
                    as: 'unit',
                    attributes: []
                }]
            })
        }

        return CourseWWTopicQuestion.findAll({
            where,
            attributes: [
                'id',
                [sequelize.literal(`'Problem ' || "${CourseWWTopicQuestion.name}".${CourseWWTopicQuestion.rawAttributes.problemNumber.field}`), 'name'],
                [sequelize.fn('avg', sequelize.col(`grades.${StudentGrade.rawAttributes.numAttempts.field}`)), 'averageAttemptedCount'],
                [sequelize.fn('avg', sequelize.col(`grades.${StudentGrade.rawAttributes.bestScore.field}`)), 'averageScore'],
                [sequelize.fn('count', sequelize.col(`grades.${StudentGrade.rawAttributes.id.field}`)), 'totalGrades'],
                [sequelize.literal(`count(CASE WHEN "grades".${StudentGrade.rawAttributes.bestScore.field} >= 1 THEN "grades".${StudentGrade.rawAttributes.id.field} END)`), 'completedCount'],
                [sequelize.literal(`CASE WHEN COUNT("grades".${StudentGrade.rawAttributes.id.field}) > 0 THEN count(CASE WHEN "grades".${StudentGrade.rawAttributes.bestScore.field} >= 1 THEN "grades".${StudentGrade.rawAttributes.id.field} END)::FLOAT / count("grades".${StudentGrade.rawAttributes.id.field}) ELSE NULL END`), 'completionPercent'],
            ],
            include,
            group: [`${CourseWWTopicQuestion.name}.${CourseWWTopicQuestion.rawAttributes.id.field}`]
        })
    }

    async getQuestions({
        courseTopicContentId,
        userId
    }: {
        courseTopicContentId: number,
        userId: number
    }) {
        try {
            const include: sequelize.IncludeOptions[] = []
            if(_.isNil(userId) === false) {
                include.push({
                    model: StudentGrade,
                    as: 'grades',
                    required: false,
                    where: {
                        userId
                    }
                })
            }

            const where: sequelize.WhereOptions = _({
                courseTopicContentId
            }).omitBy(_.isUndefined).value()

            const findOptions: sequelize.FindOptions = {
                include,
                where,
                order: [
                    ['problemNumber', 'ASC'],
                ]
            }
            return await CourseWWTopicQuestion.findAll(findOptions)
        } catch (e) {
            throw new WrappedError('Error fetching problems', e);
        }
    }

    /**
     * Get's a list of questions that are missing a grade
     * We can then go and create a course
     */
    async getQuestionsThatRequireGradesForUser({ courseId, userId}: {courseId: number; userId: number}) {
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
            })
        } catch (e) {
            throw new WrappedError('Could not getQuestionsThatRequireGradesForUser', e)
        }
    }

    /*
    * Get all users that don't have a grade on a question
    * Useful when adding a question to a course that already has enrollments
    */
    async getUsersThatRequireGradeForQuestion({ questionId } : { questionId: number }) {
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
            })
        } catch (e) {
            throw new WrappedError('Could not getUsersThatRequireGradeForQuestion', e);
        }
    }
    
    async createGradesForUserEnrollment({ courseId, userId}: {courseId: number; userId: number}) {
        const results = await this.getQuestionsThatRequireGradesForUser({
            courseId,
            userId
        })
        await results.asyncForEach(async (result) => {
            await this.createNewStudentGrade({
                courseTopicQuestionId: result.id,
                userId: userId
            })
        })
        return results.length
    }

    async createGradesForQuestion({ questionId }: { questionId: number }) {
        const results = await this.getUsersThatRequireGradeForQuestion({
            questionId
        })
        await results.asyncForEach(async (result) => {
            await this.createNewStudentGrade({
                courseTopicQuestionId: questionId,
                userId: result.userId
            })
        })
        return results.length
    }

    generateRandomSeed() {
        return Math.floor(Math.random() * 999999)
    }

    async createNewStudentGrade({
        userId,
        courseTopicQuestionId
    }: {
        userId: number,
        courseTopicQuestionId: number
    }) {
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